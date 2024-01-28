package in.succinct.catalog.indexer.configuration;

import com.venky.swf.configuration.Installer;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.reflection.ModelReflector;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.catalog.indexer.db.model.Category;
import in.succinct.catalog.indexer.db.model.Fulfillment;
import in.succinct.catalog.indexer.db.model.IndexedProviderModel;
import in.succinct.catalog.indexer.db.model.Item;
import in.succinct.catalog.indexer.db.model.Payment;
import in.succinct.catalog.indexer.db.model.Provider;
import in.succinct.catalog.indexer.db.model.ProviderLocation;

import java.util.List;

public class AppInstaller implements Installer {

    public  void install() {
        migrate(Category.class);
        migrate(Fulfillment.class);
        migrate(Item.class);
        migrate(Payment.class);
        migrate(ProviderLocation.class);
    }

    private <T extends Model & IndexedProviderModel> void migrate(Class<T> clazz) {
        ModelReflector<T> ref = ModelReflector.instance(clazz);
        List<T> list = new Select().from(clazz).where(new Expression(ref.getPool(), "SUBSCRIBER_ID", Operator.EQ)).execute(1);
        if (!list.isEmpty()) {
            List<Provider> providers = new Select().from(Provider.class).execute();
            for (Provider p : providers) {
                list = new Select().from(clazz).where(new Expression(ref.getPool(), "PROVIDER_ID", Operator.EQ, p.getId())).execute();
                for (T m : list) {
                    m.setSubscriberId(p.getSubscriberId());
                    m.save();
                }
            }
        }
    }
}

