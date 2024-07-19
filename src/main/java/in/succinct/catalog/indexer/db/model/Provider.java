package in.succinct.catalog.indexer.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.model.Model;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;

import java.util.List;

@SuppressWarnings("unused")
public interface Provider extends Model , IndexedSubscriberModel, HasDescriptor {
    public static List<Provider> findBySubscriberId(String subscriberId){
        Select select  = new Select().from(Provider.class);
        select.where(new Expression(select.getPool(),"SUBSCRIBER_ID", Operator.EQ,subscriberId));
        return select.execute();
    }
    List<ProviderLocation> getProviderLocations();
    List<Fulfillment> getFulfillments();
    List<Payment> getPayments();
    List<Category> getCategories();
    List<Item> getItems();
}
