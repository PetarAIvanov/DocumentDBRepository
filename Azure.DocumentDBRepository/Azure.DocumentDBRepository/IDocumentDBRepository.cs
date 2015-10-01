using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.DocumentDBRepository
{
    interface IDocumentDBRepository<T> where T : Document
    {
        IReliableReadWriteDocumentClient Client { get; }
    }
}
