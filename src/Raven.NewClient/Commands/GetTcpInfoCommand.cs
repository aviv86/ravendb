﻿using System;
using System.Net.Http;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetTcpInfoCommand : RavenCommand<GetTcpInfoResult>
    {
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/info/tcp";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                throw new InvalidOperationException("Got 404 / null response from querying the /info/tcp endpoint");
            }
            Result = JsonDeserializationClient.GetTcpInfoResult(response);
        }

        public override bool IsReadRequest => true;
    }
}