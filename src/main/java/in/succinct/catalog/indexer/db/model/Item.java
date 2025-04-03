package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.model.Model;

public interface Item extends Model,IndexedActivatableModel, HasDescriptor {

    @Index
    String getCategoryIds();
    void setCategoryIds(String categoryIds);

    @Index
    String getFulfillmentIds();
    void setFulfillmentIds(String fulfillmentIds);

    @Index
    String getPaymentIds();
    void setPaymentIds(String paymentIds);

    @Index
    String getLocationIds();
    void setLocationIds(String locationIds);


}
