using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.KafkaDemo.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configSetup = BootstrapSetup.Create().WithConfig(KafkaExtensions.DefaultSettings);
            var actorSystem = ActorSystem.Create("KafkaSpec", configSetup);
            var materializer = actorSystem.Materializer();
            
            var kafkaHost = Environment.GetEnvironmentVariable("KAFKA_HOST") ?? "localhost";
            var kafkaPort = int.Parse(Environment.GetEnvironmentVariable("KAFKA_PORT") ?? "29092");

            var kafkaUserSasl = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
            var kafkaUserPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");

            var hasSasl = !(string.IsNullOrEmpty(kafkaUserSasl) || string.IsNullOrEmpty(kafkaUserPassword));
            
            var producerConfig = new ProducerConfig()
            {
            };

            if (hasSasl)
            {
                producerConfig.SaslMechanism = SaslMechanism.Plain;
                producerConfig.SaslUsername = kafkaUserSasl;
                producerConfig.SaslPassword = kafkaUserPassword;
                producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            }

            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}");
            
            producerConfig.ForEach(kv => producerSettings = producerSettings.WithProperty(kv.Key, kv.Value));
            
            await BeginProduce(producerSettings, materializer);

            await actorSystem.Terminate();
        }
        
        private static async Task BeginProduce(ProducerSettings<Null, string> producerSettings, ActorMaterializer materializer)
        {
            await Source
                .Cycle(() => Enumerable.Range(1, 100).GetEnumerator())
                .Select(c => c.ToString())
                .Select(elem => ProducerMessage.Single(new ProducerRecord<Null, string>("akka-input", elem)))
                .WithAttributes(Attributes.CreateName("FlexiFlow-outbound"))
                .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(producerSettings))
                .GroupedWithin(10000, TimeSpan.FromSeconds(1))
                .Select(c => $"{c.Count()} items")
                .Log("Producer").WithAttributes(Attributes.CreateLogLevels(LogLevel.InfoLevel))
                .RunWith(Sink.Ignore<string>(), materializer).ConfigureAwait(false);
        }
    }
}