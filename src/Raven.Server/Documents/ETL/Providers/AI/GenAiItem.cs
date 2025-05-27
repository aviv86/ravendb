namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class GenAiItem : ExtractedItem
{
    public GenAiItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
           
    }

    public GenAiItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
    {
    }
}
