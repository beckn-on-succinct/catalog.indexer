package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.IS_NULLABLE;
import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;

public interface IndexedProviderModel extends IndexedSubscriberModel {

    @Index
    @IS_NULLABLE(value = false)
    @UNIQUE_KEY
    public Long getProviderId();
    public void setProviderId(Long id);
    public Provider getProvider();



}
