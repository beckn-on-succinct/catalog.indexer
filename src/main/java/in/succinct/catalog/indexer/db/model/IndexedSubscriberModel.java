package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.COLUMN_SIZE;
import com.venky.swf.db.annotations.column.IS_NULLABLE;
import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.annotations.model.HAS_DESCRIPTION_FIELD;
import com.venky.swf.db.model.application.Application;

@HAS_DESCRIPTION_FIELD("OBJECT_NAME")
public interface IndexedSubscriberModel {
    @UNIQUE_KEY
    @Index
    @IS_NULLABLE(value = true)
    /*
    BPP Id
     */
    public String getSubscriberId();
    public void setSubscriberId(String bppId);


    @UNIQUE_KEY
    @Index
    public String getObjectId();
    public void setObjectId(String objectId);

    @Index
    public String getObjectName();
    public void setObjectName(String objectName);


    @COLUMN_SIZE(1024*100)
    public String getObjectJson();
    public void setObjectJson(String objectJson);
}
