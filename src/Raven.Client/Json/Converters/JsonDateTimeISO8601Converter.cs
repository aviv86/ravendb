using System;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Json.Converters
{
    internal sealed class JsonDateTimeISO8601Converter : RavenJsonConverter
    {
        public static readonly JsonDateTimeISO8601Converter Instance = new JsonDateTimeISO8601Converter();

        private JsonDateTimeISO8601Converter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value is DateTime dateTime)
            {
                switch (serializer.DateTimeZoneHandling)
                {
                    case DateTimeZoneHandling.RoundtripKind:
                        break;
                    case DateTimeZoneHandling.Unspecified:
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);
                        break;
                    case DateTimeZoneHandling.Local:
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                        break;
                    case DateTimeZoneHandling.Utc:
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                writer.WriteValue(dateTime.GetDefaultRavenFormat(dateTime.Kind == DateTimeKind.Utc));
            }
            else if (value is DateTimeOffset)
            {
                var dateTimeOffset = (DateTimeOffset)value;
                writer.WriteValue(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
            }
            else
                throw new ArgumentException(string.Format("Not idea how to process argument: '{0}'", value));
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value is string s)
            {
                if (objectType == typeof(DateTime) || objectType == typeof(DateTime?))
                {
                    DateTime time;
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out time))
                    {
                        if (s.EndsWith("+00:00"))
                            return time.ToUniversalTime();
                        return time;
                    }
                }
                if (objectType == typeof(DateTimeOffset) || objectType == typeof(DateTimeOffset?))
                {
                    DateTimeOffset time;
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out time))
                        return time;
                }

            }
            return DeferReadToNextConverter(reader, objectType, serializer, existingValue);
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(DateTime) == objectType ||
                typeof(DateTimeOffset) == objectType ||
                typeof(DateTimeOffset?) == objectType ||
                typeof(DateTime?) == objectType;
        }
    }
}
