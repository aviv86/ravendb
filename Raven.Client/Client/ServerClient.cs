using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;

namespace Raven.Client.Client
{
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;

		public ServerClient(string server, int port)
		{
			url = String.Format("http://{0}:{1}", server, port);
		}

		#region IDatabaseCommands Members

		public JsonDocument Get(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

		    var metadata = new JObject();
		    AddTransactionInformation(metadata);
			var request = new HttpJsonRequest(url + "/docs/" + key, "GET", metadata);
			return new JsonDocument
			{
				Data = Encoding.UTF8.GetBytes(request.ReadResponseString()),
				Key = key,
                Etag = new Guid(request.ResponseHeaders["ETag"]),
                Metadata = request.ResponseHeaders.FilterHeaders()
			};
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if(string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
            if (metadata == null)
                metadata = new JObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
            AddTransactionInformation(metadata);
            if (etag != null)
                metadata["ETag"] = new JValue(etag.Value.ToString());
		    var request = new HttpJsonRequest(url + "/docs/" + key, method, metadata);
			request.Write(document.ToString());

		    string readResponseString;
		    try
		    {
		        readResponseString = request.ReadResponseString();
		    }
		    catch (WebException e)
		    {
                var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null ||
                    httpWebResponse.StatusCode != HttpStatusCode.Conflict)
                    throw;
                throw ThrowConcurrencyException(e);
		    }
		    return JsonConvert.DeserializeObject<PutResult>(readResponseString);
		}

	    private static void AddTransactionInformation(JObject metadata)
	    {
	        if (Transaction.Current == null) 
                return;

	        string txInfo = Transaction.Current.TransactionInformation.DistributedIdentifier + ", " +
	                        TransactionManager.DefaultTimeout.ToString("c");
	        metadata["Raven-Transaction-Information"] = new JValue(txInfo);
	    }

	    public void Delete(string key, Guid? etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
	        var metadata = new JObject();
            if (etag != null)
                metadata.Add("ETag", new JValue(etag.Value.ToString()));
	        AddTransactionInformation(metadata);
	        var httpJsonRequest = new HttpJsonRequest(url + "/docs/" + key, "DELETE", metadata);
	        try
	        {
	            httpJsonRequest.ReadResponseString();
	        }
	        catch (WebException e)
	        {
	            var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null ||
                    httpWebResponse.StatusCode != HttpStatusCode.Conflict)
                    throw;
                 throw ThrowConcurrencyException(e);
	        }
		}

	    private static Exception ThrowConcurrencyException(WebException e)
	    {
	        using (var sr = new StreamReader(e.Response.GetResponseStream()))
	        {
	            var text = sr.ReadToEnd();
	            var errorResults = JsonConvert.DeserializeAnonymousType(text, new
	            {
	                url = (string) null,
	                actualETag = Guid.Empty,
	                expectedETag = Guid.Empty,
	                error = (string) null
	            });
	            return new ConcurrencyException(errorResults.error)
	            {
	                ActualETag = errorResults.actualETag,
	                ExpectedETag = errorResults.expectedETag
	            };
	        }
	    }

	    public string PutIndex(string name, string indexDef)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			var request = new HttpJsonRequest(url + "/indexes/" + name, "PUT");
			request.Write(indexDef);

			var obj = new {index = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

		public QueryResult Query(string index, string query, int start, int pageSize)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = url + "/indexes/" + index + "?query=" + query + "&start=" + start + "&pageSize=" + pageSize;
			var request = new HttpJsonRequest(path, "GET");
			var serializer = new JsonSerializer();
			JToken json;
			using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
				json = (JToken) serializer.Deserialize(reader);

			return new QueryResult
			{
				IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
				Results = json["Results"].Children().Cast<JObject>().ToArray(),
			};
		}

		public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			throw new NotImplementedException();
		}

		public JArray GetDocuments(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		public JArray GetIndexNames(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		public JArray GetIndexes(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

	    public void Commit(Guid txId)
	    {
	        throw new NotImplementedException();
	    }

	    public void Rollback(Guid txId)
	    {
	        throw new NotImplementedException();
	    }

	    #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}