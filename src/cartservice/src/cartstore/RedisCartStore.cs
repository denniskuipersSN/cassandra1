// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using OpenTelemetry.Trace;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using System.Diagnostics;
using StackExchange.Redis;
using Google.Protobuf;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading;
using Microsoft.Extensions.Options;


namespace cartservice.cartstore
{
    public class RedisCartStore : ICartStore
    {

        readonly ILogger<RedisCartStore> _logger;
        readonly ILoggerFactory _loggerFactory;

        private DateTime prev_date;
        private DateTime slowWindow;
        private DateTime prev_slowWindow;

        private TimeSpan tsSW;

        private const string CART_FIELD_NAME = "cart";
        private const int REDIS_RETRY_NUM = 30;

        private volatile ConnectionMultiplexer redis;
        private volatile bool isRedisConnectionOpened = false;

        private readonly object locker = new object();
        private readonly byte[] emptyCartBytes;
        private readonly string connectionString;

        private readonly ConfigurationOptions redisConnectionOptions;

        private static ActivitySource source = new ActivitySource("cartservice.*");

        public RedisCartStore(string redisAddress)
        {
            prev_date = DateTime.Now;

            _loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.ClearProviders();
                builder.AddJsonConsole();
            });        
            _logger = _loggerFactory.CreateLogger<RedisCartStore>();


            // Serialize empty cart into byte array.
            var cart = new Hipstershop.Cart();
            emptyCartBytes = cart.ToByteArray();
            connectionString = $"{redisAddress},ssl=false,allowAdmin=true,abortConnect=false";

            redisConnectionOptions = ConfigurationOptions.Parse(connectionString);

            // Try to reconnect multiple times if the first retry fails.
            redisConnectionOptions.ConnectRetry = REDIS_RETRY_NUM;
            redisConnectionOptions.ReconnectRetryPolicy = new ExponentialRetry(1000);

            redisConnectionOptions.KeepAlive = 180;
        }

        public ConnectionMultiplexer GetConnection()
        {
            EnsureRedisConnected();
            return redis;
        }

        public Task InitializeAsync()
        {
            EnsureRedisConnected();
            return Task.CompletedTask;
        }

        private void EnsureRedisConnected()
        {
            if (isRedisConnectionOpened)
            {
                return;
            }

            // Connection is closed or failed - open a new one but only at the first thread
            lock (locker)
            {
                if (isRedisConnectionOpened)
                {
                    return;
                }

                Console.WriteLine("Connecting to Redis: " + connectionString);

                redis = ConnectionMultiplexer.Connect(redisConnectionOptions);

                if (redis == null || !redis.IsConnected)
                {
                    Console.WriteLine("Wasn't able to connect to redis");

                    // We weren't able to connect to Redis despite some retries with exponential backoff.
                    throw new ApplicationException("Wasn't able to connect to redis");
                }

                Console.WriteLine("Successfully connected to Redis");
                var cache = redis.GetDatabase();

                Console.WriteLine("Performing small test");
                cache.StringSet("cart", "OK" );
                object res = cache.StringGet("cart");
                Console.WriteLine($"Small test result: {res}");

                redis.InternalError += (o, e) => { Console.WriteLine(e.Exception); };
                redis.ConnectionRestored += (o, e) =>
                {
                    isRedisConnectionOpened = true;
                    Console.WriteLine("Connection to redis was retored successfully");
                };
                redis.ConnectionFailed += (o, e) =>
                {
                    Console.WriteLine("Connection failed. Disposing the object");
                    isRedisConnectionOpened = false;
                };

                isRedisConnectionOpened = true;
            }
        }
        public void mySlowFunction(float baseNumber, Boolean veryslow) {
            double durationSec = 0;
            String DisplayName = "";
        
            //Custom Instrumentation example
            using (Activity activity = source.StartActivity("mySlowFunction"))
            {            
              activity?.AddTag("Veryslow",baseNumber.ToString());
              if (veryslow)
              {
                _logger.LogError(DateTime.Now.ToString() + "Error Slowdown Is using SpinWait and Thread sleep in spam id : " + activity.Id );
                Thread.SpinWait(100000000);
                Thread.Sleep(4000);
              }
	          double result = 0;	
	          for (var i = Math.Pow(baseNumber, 2); i >= 0; i--) {		
		          result += Math.Atan(i) * Math.Tan(i);
	          };
              activity.Stop();   
              durationSec = activity.Duration.TotalSeconds;
              DisplayName = activity.DisplayName;           
            }
            Console.Out.WriteLine("mySlowFunction took : " + durationSec.ToString());
            _logger.LogError(DateTime.Now.ToString() + " mySlowFunction took " + durationSec.ToString() + " seconds" );
            if (durationSec >= 1 && veryslow)
                _logger.LogError(DateTime.Now.ToString() + " Fatale Error in Span " + DisplayName + " took " + durationSec.ToString() + " seconds" );
            else
                _logger.LogInformation(DateTime.Now.ToString() + " Info : Span " + DisplayName + " took " + durationSec.ToString() + " seconds" );
        }

        public async Task AddItemAsync(string userId, string productId, int quantity)
        {
            Console.WriteLine($"AddItemAsync called with userId={userId}, productId={productId}, quantity={quantity}");

            float total = 150*quantity;
            Boolean makeitverslow = false;
            DateTime current_date = DateTime.Now;

            TimeSpan ts = current_date - prev_date;
            double DiffMin = ts.TotalMinutes;
             
            _logger.LogInformation(DateTime.Now.ToString() + " Info TS : " + ts.TotalMinutes.ToString() + " TSW : " + tsSW.TotalMinutes.ToString() + " Product " + productId + " Total :" + total); 

            //if ((total == 1500 || total == 750) && (productId.Equals("6E92ZMYYFZ") || productId.Equals("0PUK6V6EV0") || productId.Equals("9SIQT8TOJO") ||  productId.Equals("2ZYFJ3GM2N")))
            if ((total == 1500 || total == 750) && (productId.Equals("6E92ZMYYFZ") || productId.Equals("0PUK6V6EV0") || productId.Equals("9SIQT8TOJO") ||  productId.Equals("2ZYFJ3GM2N")) && DiffMin >= 15 && (DiffMin + 10) <= 35)
            {
               makeitverslow = true;
               total = 2508;
               
            }
            mySlowFunction(total,makeitverslow);

            if(DiffMin >= 15 && (DiffMin + 10) >= 35)
            {
                prev_date = DateTime.Now;
            } 
        
            try
            {
                EnsureRedisConnected();

                var db = redis.GetDatabase();

                // Access the cart from the cache
                var value = await db.HashGetAsync(userId, CART_FIELD_NAME);

                Hipstershop.Cart cart;
                if (value.IsNull)
                {
                    cart = new Hipstershop.Cart();
                    cart.UserId = userId;
                    cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                }
                else
                {
                    cart = Hipstershop.Cart.Parser.ParseFrom(value);
                    var existingItem = cart.Items.SingleOrDefault(i => i.ProductId == productId);
                    if (existingItem == null)
                    {
                        cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                    }
                    else
                    {
                        existingItem.Quantity += quantity;
                    }
                }

                await db.HashSetAsync(userId, new[]{ new HashEntry(CART_FIELD_NAME, cart.ToByteArray()) });
            }
            catch (Exception ex)
            {
                throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
            }
        }

        public async Task EmptyCartAsync(string userId)
        {
            Console.WriteLine($"EmptyCartAsync called with userId={userId}");

            try
            {
                EnsureRedisConnected();
                var db = redis.GetDatabase();

                // Update the cache with empty cart for given user
                await db.HashSetAsync(userId, new[] { new HashEntry(CART_FIELD_NAME, emptyCartBytes) });
            }
            catch (Exception ex)
            {
                throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
            }
        }

        public async Task<Hipstershop.Cart> GetCartAsync(string userId)
        {
            Console.WriteLine($"GetCartAsync called with userId={userId}");

            try
            {
                EnsureRedisConnected();

                var db = redis.GetDatabase();

                // Access the cart from the cache
                var value = await db.HashGetAsync(userId, CART_FIELD_NAME);

                if (!value.IsNull)
                {
                    return Hipstershop.Cart.Parser.ParseFrom(value);
                }

                // We decided to return empty cart in cases when user wasn't in the cache before
                return new Hipstershop.Cart();
            }
            catch (Exception ex)
            {
                throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.FailedPrecondition, $"Can't access cart storage. {ex}"));
            }
        }

        public bool Ping()
        {
            try
            {
                var cache = redis.GetDatabase();
                var res = cache.Ping();
                return res != TimeSpan.Zero;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}