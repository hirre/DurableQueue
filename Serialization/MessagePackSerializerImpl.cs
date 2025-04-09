using DurableQueue.Interfaces;
using MessagePack;

namespace DurableQueue.Serialization
{
    public class MessagePackSerializerImpl : ISerializer
    {
        public byte[] Serialize<T>(T obj)
        {
            return MessagePackSerializer.Serialize(obj);
        }
        public T Deserialize<T>(byte[] data)
        {
            return MessagePackSerializer.Deserialize<T>(data);
        }
    }
}
