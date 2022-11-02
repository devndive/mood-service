import Fastify, { FastifyInstance, RouteShorthandOptions } from 'fastify';
import FastifyEnv from '@fastify/env';
import { Server, IncomingMessage, ServerResponse } from 'http';
import { CosmosClient } from '@azure/cosmos';
import { createClient } from 'redis';

import { Static, Type } from '@sinclair/typebox'

import metricsPlugin from 'fastify-metrics';

declare module 'fastify' {
  interface FastifyInstance {
    config: {
      COSMOS_DB_ENDPOINT: 'string',
      COSMOS_DB_KEY: 'string',
      REDIS_URL: 'string',
      PORT: 'number',
    };
  }
}

const ConfidenceScores = Type.Object({
  positive: Type.Number(),
  neutral: Type.Number(),
  negative: Type.Number(),
});

const Sentence = Type.Object({
  sentiment: Type.String(),
  confidenceScores: ConfidenceScores,
  offset: Type.Integer(),
  length: Type.Integer(),
  text: Type.String(),
});

const Sentiment = Type.Object({
  sentiment: Type.String(),
  confidenceScores: ConfidenceScores,
  sentences: Type.Array(Sentence),
});

const TweetWithSentiment = Type.Object({
  id: Type.String(),
  text: Type.String(),
  sentiment: Sentiment,
});

type TweetWithSentimentType = Static<typeof TweetWithSentiment>;

const SentimentResponse = Type.Object({
  data: Type.Array(TweetWithSentiment)
})

type SentimentResponseType = Static<typeof SentimentResponse>;

const schema = {
  type: 'object',
  required: ['COSMOS_DB_ENDPOINT', 'COSMOS_DB_KEY', 'REDIS_URL', 'PORT'],
  properties: {
    COSMOS_DB_ENDPOINT: { type: 'string' },
    COSMOS_DB_KEY: { type: 'string' },
    REDIS_URL: { type: 'string' },
    PORT: { type: 'number', default: 3100 },
  },
};

const options = {
  schema,
  dotenv: true,
  logger: true,
};

const server: FastifyInstance<Server, IncomingMessage, ServerResponse> = Fastify({}).register(FastifyEnv, options);

const opts: RouteShorthandOptions<Server, IncomingMessage, ServerResponse> = {
  schema: {
    response: {
      200: SentimentResponse,
    }
  }
};

let client = createClient({
  url: process.env.REDIS_URL,
});


server.get<{ Body: null; Reply: SentimentResponseType }>('/sentiment', opts, async (request, reply) => {
  let tweets: TweetWithSentimentType[] = [];

  const timeLabel = `request-${request.id}`;

  console.time(`${timeLabel}`);
  const tweetsFromCache = await client.get('tweets');
  console.timeLog(`${timeLabel}`, 'redis');

  console.log('tweetsFromCache', tweetsFromCache);
  if (tweetsFromCache) {
    tweets = JSON.parse(tweetsFromCache);
  }

  if (tweets.length === 0) {
    const cosmosClient = new CosmosClient({
      endpoint: server.config.COSMOS_DB_ENDPOINT || "",
      key: server.config.COSMOS_DB_KEY,
    });

    const querySpec = {
      query: "SELECT * FROM tweets t WHERE t.type = @type",
      parameters: [
        {
          name: "@type",
          value: "tweet"
        }
      ]
    };

    const response = await cosmosClient
      .database('mood')
      .container('tweets')
      .items.query(querySpec).fetchAll();

    if (response.resources.length > 0) {
      tweets = response.resources.map(tweetWithSentiment => {

        return {
          id: tweetWithSentiment.id as string,
          text: tweetWithSentiment.text as string,
          sentiment: {
            sentiment: tweetWithSentiment.sentiment.sentiment as string,
            confidenceScores: {
              positive: tweetWithSentiment.sentiment.confidenceScores.positive as number,
              neutral: tweetWithSentiment.sentiment.confidenceScores.neutral as number,
              negative: tweetWithSentiment.sentiment.confidenceScores.negative as number,
            },
            sentences: tweetWithSentiment.sentiment.sentences.map((s: any) => {
              return {
                sentiment: s.sentiment as string,
                confidenceScores: {
                  positive: s.confidenceScores.positive as number,
                  neutral: s.confidenceScores.neutral as number,
                  negative: s.confidenceScores.negative as number,
                },
                offset: s.offset as number,
                length: s.length as number,
                text: s.text as string,
              }
            })
          }
        }

      });

      console.timeLog(`${timeLabel}`, 'done mapping');
      await client.set('tweets', JSON.stringify(tweets), { EX: 60 * 60 * 24 });
      console.timeLog(`${timeLabel}`, 'written to redis');
    }
  }

  console.timeEnd(`${timeLabel}`);
  reply.status(200).send({ data: tweets });
});

const PostSentimentRequestBody = Type.Object({
  data: Type.Array(TweetWithSentiment)
});

type PostSentimentRequestBodyType = Static<typeof PostSentimentRequestBody>;

const PostSentimentResponse = Type.Object({})
type PostSentimentResponseType = Static<typeof PostSentimentResponse>;

const postSentimentOpts: RouteShorthandOptions<Server, IncomingMessage, ServerResponse> = {
  schema: {
    body: {
      type: 'object',
      required: ['data'],
      properties: {
        data: {
          type: 'array',
          items: {
            type: 'object',
            required: ['id', 'text', 'sentiment'],
            properties: {
              id: { type: 'string' },
              text: { type: 'string' },
              sentiment: {
                type: 'object',
                required: ['sentiment', 'confidenceScores'],
                properties: {
                  sentiment: { type: 'string' },
                  confidenceScores: {
                    type: 'object',
                    required: ['positive', 'neutral', 'negative'],
                    properties: {
                      positive: { type: 'number' },
                      neutral: { type: 'number' },
                      negative: { type: 'number' },
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    response: {
      200: PostSentimentResponse,
    }
  }
};

server.post<{ Body: PostSentimentRequestBodyType; Reply: PostSentimentResponseType }>('/sentiment', postSentimentOpts, async (request, reply) => {
  console.log(request.body);

  const cosmosClient = new CosmosClient({
    endpoint: server.config.COSMOS_DB_ENDPOINT || "",
    key: server.config.COSMOS_DB_KEY,
  });

  const tweets = request.body.data;
  if (tweets && tweets.length > 0) {
    for (const tweet of tweets) {
      await cosmosClient
        .database('mood')
        .container('tweets')
        .items.upsert({
          ...tweet,
          type: "tweet",
        });
    }
  }

  reply.status(200).send({ "status": "ok" });
});

const LastKnownTweet = Type.Object({
  id: Type.String(),
});

const LastKnownTweetResponse = Type.Object({
  data: LastKnownTweet
})

type LastKnownTweetResponseType = Static<typeof LastKnownTweetResponse>;

const lastKnownTweetOpts: RouteShorthandOptions<Server, IncomingMessage, ServerResponse> = {
  schema: {
    response: {
      200: LastKnownTweetResponse,
    }
  }
};

server.get<{ Body: null; Reply: LastKnownTweetResponseType }>('/last-known-tweet', lastKnownTweetOpts, async (request, reply) => {
  console.log("Getting highest tweet id");
  const cosmosClient = new CosmosClient({
    endpoint: server.config.COSMOS_DB_ENDPOINT || "",
    key: server.config.COSMOS_DB_KEY,
  });

  const querySpec = {
    query: "SELECT t.id FROM tweets t WHERE t.type = @type ORDER BY t.id DESC OFFSET 0 LIMIT 1",
    parameters: [
      {
        name: "@type",
        value: "tweet"
      }
    ]
  };

  const response = await cosmosClient
    .database('mood')
    .container('tweets')
    .items.query(querySpec).fetchAll();

  if (response.resources.length > 0) {
    console.log("Highest tweet id: " + response.resources[0].id);
    reply.status(200).send({ data: { id: response.resources[0].id } });
    console.log("Last known tweet id: " + response.resources[0].id);
  } else {
    reply.status(404).send({ data: { id: '' } });
  }
});

const start = async () => {
  server.addContentTypeParser('text/json', { parseAs: 'string' }, server.getDefaultJsonParser('ignore', 'ignore'))
  server.register(metricsPlugin, { endpoint: '/metrics' });

  await server.ready();
  // @ts-ignore
  server.listen({ port: server.config.PORT, host: "::" });

  client = createClient({
    url: server.config.REDIS_URL,
  });

  console.log(`Server listening on ${server.config.PORT}`);
  await client.connect();
};

start();
