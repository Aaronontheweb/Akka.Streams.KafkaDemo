using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Aaron.Akka.Streams.Dsl;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.KafkaDemo.Consumer
{
    class Program
    {
         static async Task Main(string[] args)
         {
             var config = ConfigurationFactory.ParseString(@"akka.loglevel = INFO");
            var configSetup = BootstrapSetup.Create().WithConfig(config
                .WithFallback(KafkaExtensions.DefaultSettings));
            var actorSystem = ActorSystem.Create("KafkaSpec", configSetup);
            var materializer = actorSystem.Materializer();

            var kafkaHost = Environment.GetEnvironmentVariable("KAFKA_HOST") ?? "localhost";
            var kafkaPort = int.Parse(Environment.GetEnvironmentVariable("KAFKA_PORT") ?? "29092");

            var kafkaUserSasl = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
            var kafkaUserPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");

            var hasSasl = !(string.IsNullOrEmpty(kafkaUserSasl) || string.IsNullOrEmpty(kafkaUserPassword));
            
            var consumerConfig = new ConsumerConfig
            {
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = true,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ClientId = "substream.client",
                SocketKeepaliveEnable = true,
                ConnectionsMaxIdleMs = 180000,
            };

            var consumerSettings = ConsumerSettings<Null, string>
                .Create(actorSystem, null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}")
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithPollTimeout(TimeSpan.FromMilliseconds(100))
                .WithGroupId("group2");

            if (hasSasl)
            {
                actorSystem.Log.Info("Using SASL...");
                consumerConfig.SaslMechanism = SaslMechanism.Plain;
                consumerConfig.SaslUsername = kafkaUserSasl;
                consumerConfig.SaslPassword = kafkaUserPassword;
                consumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            }
            
            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}");
            
            consumerConfig.ForEach(kv => consumerSettings = consumerSettings.WithProperty(kv.Key, kv.Value));

            // create a custom Decider with a "Restart" directive in the event of DivideByZeroException
            Akka.Streams.Supervision.Decider decider = cause => Akka.Streams.Supervision.Directive.Restart;

            IAsyncEnumerable<int> enumer = KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics("akka-input"))
                .WithAttributes(Attributes.CreateName("PlainSource")
                    .And(ActorAttributes.CreateSupervisionStrategy(decider)))
                .Select(c => (c.Topic, c.Message.Value))
                .GroupedWithin(100000, TimeSpan.FromSeconds(1))
                .Select(c => c.Count())
                .RunAsAsyncEnumerable(materializer);

            await foreach (var msgPerSec in enumer)
            {
                actorSystem.Log.Info("msg/s: {0}", msgPerSec);
            }
            
            await actorSystem.WhenTerminated;
        }
    }
}