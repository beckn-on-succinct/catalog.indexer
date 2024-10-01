package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.model.Model;

public interface ProviderTag extends Model {
    @UNIQUE_KEY
    @Index
    Long getProviderId();
    void setProviderId(Long id);
    Provider getProvider();

    @UNIQUE_KEY
    @Index
    String getTagGroupCode();
    void setTagGroupCode(String tagGroupName);

    @UNIQUE_KEY
    @Index
    String getTagCode();
    void setTagCode(String tagName);

    @Index
    String getTagValue();
    void setTagValue(String tagValue);

}
