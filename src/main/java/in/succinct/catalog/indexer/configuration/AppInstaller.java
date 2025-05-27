package in.succinct.catalog.indexer.configuration;

import com.venky.swf.configuration.Installer;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.reflection.ModelReflector;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import com.venky.swf.sql.parser.SQLExpressionParser.EQ;
import in.succinct.catalog.indexer.db.model.Category;
import in.succinct.catalog.indexer.db.model.Fulfillment;
import in.succinct.catalog.indexer.db.model.IndexedProviderModel;
import in.succinct.catalog.indexer.db.model.Item;
import in.succinct.catalog.indexer.db.model.Payment;
import in.succinct.catalog.indexer.db.model.Provider;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import in.succinct.onet.core.adaptor.NetworkAdaptor.Domain;
import in.succinct.onet.core.adaptor.NetworkAdaptor.DomainCategory;
import in.succinct.onet.core.adaptor.NetworkAdaptorFactory;

import java.sql.Timestamp;
import java.util.List;

public class AppInstaller implements Installer {

    public  void install() {
        migrate(Category.class);
        migrate(Fulfillment.class);
        migrate(Item.class);
        migrate(Payment.class);
        migrate(ProviderLocation.class);
        fixServiceRegions();
        migrateDomain();
    }
    
    private void migrateDomain() {
        Select select = new Select().from(Item.class);
        select.where(new Expression(select.getPool(),"DOMAIN", Operator.EQ));
        List<Item> items = select.execute();
        if (items.isEmpty()){
            return;
        }
        NetworkAdaptor networkAdaptor  = NetworkAdaptorFactory.getInstance().getAdaptor();
        Domain defaultDomain = null;
        for (Domain domain : networkAdaptor.getDomains()) {
            DomainCategory category = domain.getDomainCategory();
            if (category == DomainCategory.BUY_MOVABLE_GOODS){
                defaultDomain = domain;
                break;
            }
        }
        
        if (defaultDomain != null) {
            for (Item item : items) {
                item.setDomain(defaultDomain.getId());
                item.save();
            }
        }
    }
    
    private void fixServiceRegions() {
        Select select = new Select().from(ProviderLocation.class);
        select.where(new Expression(select.getPool(), Conjunction.OR).
                add(new Expression(select.getPool(),"MAX_LAT", Operator.EQ)).
                add(new Expression(select.getPool(),"LAT", Operator.EQ)));
        List<ProviderLocation> providerLocations = select.execute();
        for (ProviderLocation pl : providerLocations){
            pl.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
            pl.save();
        }
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

