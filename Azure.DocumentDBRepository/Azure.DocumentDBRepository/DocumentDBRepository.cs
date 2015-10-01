using Azure.DocumentDBRepository.Util;
using Microsoft.Azure;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Client.TransientFaultHandling;
using Microsoft.Azure.Documents.Partitioning;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.DocumentDBRepository
{
    public class DocumentDBRepository<T> : IDocumentDBRepository<T> where T : Document, new()
    {
        //TODO: put these in a config class
        private string _endpoint;
        private string _authKey;
        private string _databaseId;
        private string _collectionPrefix;
        private int _partitionCount = 0;
        private Func<T, string> _partitionKeyExtractor;
        private ConnectionPolicy _clientConnectionPolicy;

        public DocumentDBRepository(string endpoint, string authKey,
            string databaseId, string collectionPrefix, int partitionCount = 1,
            Func<object, string> partitionKeyExtractor = null,
            ConnectionPolicy clientConnectionPolicy = null)
        {
            this._endpoint = endpoint;
            this._authKey = authKey;
            this._databaseId = databaseId;
            this._collectionPrefix = collectionPrefix;
            this._partitionCount = partitionCount;
            this._partitionKeyExtractor = partitionKeyExtractor != null ? partitionKeyExtractor : u => ((Document)u).Id;
            this._clientConnectionPolicy = clientConnectionPolicy;

            //instantiate the partition resolver
            var a = PartitionResolver;
        }

        #region Public Properties
        /// <summary>
        /// The _client 
        /// </summary>
        private static IReliableReadWriteDocumentClient _client;
        public IReliableReadWriteDocumentClient Client
        {
            get
            {
                if (_client == null)
                {
                    _client = DocumentDbUtil.CreateClient(_endpoint, _authKey).Result;
                }

                return _client;
            }
        }

        private Database _database;
        public Database Database
        {
            get
            {
                if (_database == null)
                {
                    _database = DocumentDbUtil.GetOrCreateDatabaseAsync(Client, _databaseId).Result;
                }

                return _database;
            }
        }

        private IEnumerable<DocumentCollection> _partitionCollections;
        public IEnumerable<DocumentCollection> PartitionCollections
        {
            get
            {
                if (_partitionCollections == null)
                {
                    _partitionCollections = DocumentDbUtil.GetOrCreatePartitionCollections(Client,
                        Database, _collectionPrefix, _partitionCount).Result;
                }

                return _partitionCollections;
            }
        }

        public IEnumerable<string> PartitionCollectionsSelfLinks
        {
            get
            {
                return PartitionCollections.Select(x => x.SelfLink);
            }
        }

        private IPartitionResolver _partitionResolver;
        public IPartitionResolver PartitionResolver
        {
            get
            {
                if (_partitionResolver == null)
                {
                    //for starters, we are going to create a HashPartitionResolver using 
                    //the Id property which is a property of the Document class.
                    // This however adds a requirement that we generate an ID (guid) prior to inserting 
                    // a new document
                    HashPartitionResolver hashResolver = new HashPartitionResolver((Func<object, string>)_partitionKeyExtractor, PartitionCollectionsSelfLinks);
                    Client.UnderlyingClient.PartitionResolvers[Database.SelfLink] = hashResolver;
                    _partitionResolver = hashResolver;
                }
                return _partitionResolver;
            }
        }
        #endregion

        #region CRUD Methods
        public T GetById(string id, FeedOptions feedOptions = null)
        {
            T document = Activator.CreateInstance<T>();
            document.Id = id;
            // This is essentially a test for the default key extractor - the document ID.
            // If this is the case we can extract the partition key and get away with a 
            // call to only 

            var partitionKey = PartitionResolver.GetPartitionKey(document);
            if (partitionKey != null)
            {
                return Client.CreateDocumentQuery<T>(Database.SelfLink, partitionKey: partitionKey).Where(x => x.Id == id)
                    .AsEnumerable().FirstOrDefault();
            }


            //we will execute queries against all collections and return first found result
            foreach (var coll in PartitionCollectionsSelfLinks)
            {
                var result = Client.CreateDocumentQuery<T>(coll).Where(x => x.Id == id).AsEnumerable().FirstOrDefault();
                if (result != null)
                {
                    return result;
                }
            }

            return null;
        }

        /// <summary>
        /// Executes a query using the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate.</param>
        /// <param name="options">The options.</param>
        /// <returns></returns>
        public IEnumerable<T> Get(Func<T, bool> predicate, FeedOptions options = null)
        {
            var result = new List<T>();

            //we will execute queries against all collections and return first found result
            foreach (var coll in PartitionCollectionsSelfLinks)
            {
                options = options != null ? options : new FeedOptions();

                var batch = Client.CreateDocumentQuery<T>(coll, options).Where(predicate);
                result.AddRange(batch);
            }
            return result;
        }

        /// <summary>
        /// Executes a query using the specified SqlQuerySpec
        /// </summary>
        /// <param name="spec">The spec.</param>
        /// <param name="options">The options.</param>
        /// <returns></returns>
        public IEnumerable<T> Get(SqlQuerySpec spec, FeedOptions options = null)
        {
            var result = new List<T>();

            //we will execute queries against all collections and return first found result
            foreach (var coll in PartitionCollectionsSelfLinks)
            {
                options = options != null ? options : new FeedOptions();

                result.AddRange(Client.CreateDocumentQuery<T>(coll, spec, options));
            }
            return result;
        }

        public async Task<Document> InsertItemAsync(T item, RequestOptions options = null, bool disableAutomaticGeneration = false)
        {
            try
            {
                return await Client.CreateDocumentAsync(Database.SelfLink, item, options, disableAutomaticGeneration);
            }
            catch (ArgumentNullException e)
            {
                // we want to throw this one - an ArgumentNullException is generated when the partitionKey field is null.
                throw;
            }
            catch (Exception e)
            {

            }

            return null;
        }

        public async Task<bool> UpdateAsync(T item, RequestOptions options = null)
        {
            try
            {
                string selfLink = item.SelfLink;
                if (string.IsNullOrEmpty(selfLink))
                {
                    var partitionKey = PartitionResolver.GetPartitionKey(item);
                    var document = Client.CreateDocumentQuery<T>(Database.SelfLink, null, partitionKey)
                        .Where(x => x.Id == item.Id)
                       .AsEnumerable().FirstOrDefault();
                    selfLink = document.SelfLink;
                }
                await Client.ReplaceDocumentAsync(selfLink, item, options);
                return true;
            }
            catch (Exception e)
            {
            }

            return false;
        }

        public async Task<bool> DeleteAsync(string documentId)
        {
            try
            {
                var document = this.GetById(documentId);
                if (document == null)
                {
                    return false;
                }

                await Client.DeleteDocumentAsync(document.SelfLink);
                return true;
            }
            catch (Exception e)
            {

            }

            return false;
        }
        #endregion

        #region Private Methods

        #endregion
    }
}
