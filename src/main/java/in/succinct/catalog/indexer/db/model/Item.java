package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.model.Model;

public interface Item extends Model,IndexedActivatableModel {

    @Index
    @UNIQUE_KEY(allowMultipleRecordsWithNull = false)
    public Long getCategoryId();
    public void setCategoryId(Long id);
    public Category getCategory();

    @Index
    @UNIQUE_KEY(allowMultipleRecordsWithNull = false)
    public Long getFulfillmentId();
    public void setFulfillmentId(Long id);
    public Fulfillment getFulfillment();

    @Index
    @UNIQUE_KEY(allowMultipleRecordsWithNull = false)
    public Long getPaymentId();
    public void setPaymentId(Long id);
    public Payment getPayment();

    @Index
    @UNIQUE_KEY(allowMultipleRecordsWithNull = false)
    public Long getProviderLocationId();
    public void setProviderLocationId(Long id);
    public ProviderLocation getProviderLocation();


}
