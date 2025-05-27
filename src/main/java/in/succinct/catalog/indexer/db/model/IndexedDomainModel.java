package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.indexing.Index;

public interface IndexedDomainModel {
    @Index
    String getDomain();
    void setDomain(String domain);
    
}
