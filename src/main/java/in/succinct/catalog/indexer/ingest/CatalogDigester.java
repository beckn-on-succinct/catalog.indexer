package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.model.Model;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.routing.Config;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknObjectWithId;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.catalog.indexer.db.model.IndexedActivatableModel;
import in.succinct.catalog.indexer.db.model.IndexedProviderModel;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class CatalogDigester implements Task {
    public static class DigestRule  {
        private final boolean clearCatalogBeforeOperation;
        private final Operation operation;
        public DigestRule(Operation operation){
            this(operation,false);
        }
        public DigestRule(Operation operation, boolean clearCatalogBeforeOperation){
            this.operation = operation;
            this.clearCatalogBeforeOperation = clearCatalogBeforeOperation;
        }

    }
    public enum Operation {
        deactivate,
        activate,
    }

    Context context;
    Providers providers;

    public CatalogDigester(Context context, Catalog catalog){
        this.context = context;
        this.providers = catalog.getProviders();
    }

    @Override
    public void execute() {
        TypeConverter<Boolean> booleanTypeConverter  = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();
        for (Provider provider : providers) {
            if (context != null && !ObjectUtil.isVoid(context.getBppId())){
                provider.setBppId(context.getBppId());
            }
            Boolean reset = booleanTypeConverter.
                    valueOf(provider.getTags().getTag("general_attributes","catalog.indexer.reset"));

            String operation = StringUtil.
                    valueOf(provider.getTags().getTag("general_attributes","catalog.indexer.operation"));
            if (!ObjectUtil.equals(operation,Operation.deactivate.name())){
                operation = Operation.activate.name();
            }
            DigestRule digestRule = new DigestRule(Operation.valueOf(operation),reset);

            if (digestRule.clearCatalogBeforeOperation) {
                destroy(provider);
            }
            ensureProvider(provider, digestRule.operation == Operation.activate);
        }
    }
    private void destroy(Provider bProvider){
        in.succinct.catalog.indexer.db.model.Provider dbProvider =  getProvider(bProvider);
        if (!dbProvider.getRawRecord().isNewRecord()){
            dbProvider.destroy(); // Cascade will delete other things.
        }
    }

    in.succinct.catalog.indexer.db.model.Provider getProvider(Provider bProvider){
        in.succinct.catalog.indexer.db.model.Provider provider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).newRecord();
        provider.setSubscriberId(bProvider.getBppId());
        provider.setObjectId(bProvider.getId());
        provider.setObjectName(bProvider.getDescriptor().getName());
        provider.setObjectJson(bProvider.toString());

        provider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).getRefreshed(provider);
        return provider;
    }


    private <T extends Model & IndexedProviderModel> Cache<String,T> createDbCache(Class<T> clazz, in.succinct.catalog.indexer.db.model.Provider provider){
        return new Cache<>(0,0){

            @Override
            protected T getValue(String id) {
                T c = Database.getTable(clazz).newRecord();
                c.setSubscriberId(provider.getSubscriberId());
                c.setObjectId(id);
                c.setProviderId(provider.getId());
                if (!ObjectUtil.isVoid(id)){
                    c = Database.getTable(clazz).getRefreshed(c);
                    if (c.getRawRecord().isNewRecord()){
                        c.setObjectName(id);
                        c.save();
                    }
                }
                return c;
            }
        };
    }
    private static final Map<String,Class<? extends Model>> modelClassMap = new HashMap<>(){{
        put("category_ids", in.succinct.catalog.indexer.db.model.Category.class);
        put("fulfillment_ids", in.succinct.catalog.indexer.db.model.Fulfillment.class);
        put("location_ids", ProviderLocation.class);
        put("payment_ids", in.succinct.catalog.indexer.db.model.Payment.class);
    }};
    @SuppressWarnings("unchecked")
    public <T extends Model & IndexedProviderModel> Class<T> getModelClass(String name){
        return (Class<T>)modelClassMap.get(name);
    }

    public void ensureProvider(Provider bProvider, boolean active){

        Config.instance().getLogger(getClass().getName()).info("CatalogDigester: items size: " + bProvider.getItems().size());
        Items items = bProvider.getItems();bProvider.rm("items");
        Categories categories = bProvider.getCategories();bProvider.rm("categories");
        Fulfillments fulfillments = bProvider.getFulfillments();bProvider.rm("fulfillments");
        Payments payments = bProvider.getPayments();bProvider.rm("payments");
        Locations locations = bProvider.getLocations();bProvider.rm("locations");


        in.succinct.catalog.indexer.db.model.Provider provider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).newRecord();
        provider.setSubscriberId(bProvider.getBppId());
        provider.setObjectId(bProvider.getId());
        provider.setObjectName(bProvider.getDescriptor().getName());
        provider.setObjectJson(bProvider.toString());

        provider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).getRefreshed(provider);
        provider.save();

        //Map<String, in.succinct.catalog.indexer.db.model.Item> itemMap = createDbCache(in.succinct.catalog.indexer.db.model.Item.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Category> categoryMap = createDbCache(in.succinct.catalog.indexer.db.model.Category.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Fulfillment> fulfillmentMap = createDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Payment> paymentMap = createDbCache(in.succinct.catalog.indexer.db.model.Payment.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.ProviderLocation> providerLocationMap = createDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class,provider);



        if (categories != null){
            for (int j = 0 ; j < categories.size() ; j++){
                Category category = categories.get(j);
                in.succinct.catalog.indexer.db.model.Category model = ensureProviderModel(in.succinct.catalog.indexer.db.model.Category.class,provider,active,category);
                categoryMap.put(model.getObjectId(),model);
            }
        }
        if (fulfillments != null) {
            for (int j = 0; j < fulfillments.size(); j++) {
                Fulfillment fulfillment = fulfillments.get(j);
                in.succinct.catalog.indexer.db.model.Fulfillment model = ensureProviderModel(in.succinct.catalog.indexer.db.model.Fulfillment.class, provider, active, fulfillment,(fulfillmentModel, fulfillmentBecknObject) -> fulfillmentModel.setObjectName(fulfillmentBecknObject.getType()));
                fulfillmentMap.put(model.getObjectId(), model);
            }
        }
        if (payments != null) {
            for (int j = 0; j < payments.size(); j++) {
                Payment payment = payments.get(j);
                in.succinct.catalog.indexer.db.model.Payment model = ensurePayment(provider, payment);
                paymentMap.put(model.getObjectId(), model);
            }
        }
        if (locations != null) {
            for (int j = 0; j < locations.size(); j++) {
                Location location = locations.get(j);
                ProviderLocation model = ensureProviderModel(ProviderLocation.class, provider, active, location,(pl, l) -> pl.setObjectName(l.getDescriptor().getName()));
                providerLocationMap.put(model.getObjectId(), model);
            }
        }


        if (items != null) {
            for (int j = 0; j < items.size(); j++) {
                Item item = items.get(j);
                if (item.getFulfillmentIds() == null) {
                    item.setFulfillmentIds(new BecknStrings());
                    item.getFulfillmentIds().add(item.getFulfillmentId());
                }
                if (item.getLocationIds() == null){
                    item.setLocationIds(new BecknStrings());
                    item.getLocationIds().add(item.getLocationId());
                }

                if (item.getPaymentIds() == null){
                    item.setPaymentIds(new BecknStrings());
                    item.getPaymentIds().add(null);
                }
                if (item.getCategoryIds() == null){
                    item.setCategoryIds(new BecknStrings());
                    item.getCategoryIds().add(item.getCategoryId());
                }

                for (String categoryId : item.getCategoryIds()) {
                    for (String locationId : item.getLocationIds()) {
                        for (String paymentId : item.getPaymentIds()) {
                            for (String fulfillmentId : item.getFulfillmentIds()) {
                                ensureProviderModel(in.succinct.catalog.indexer.db.model.Item.class, provider, active, item, (model, becknObject) -> {
                                    if (categoryId != null ) model.setCategoryId(categoryMap.get(categoryId).getId());
                                    if (locationId != null ) model.setProviderLocationId(providerLocationMap.get(locationId).getId());
                                    if (paymentId != null) model.setPaymentId(paymentMap.get(paymentId).getId());
                                    if (fulfillmentId !=null) model.setFulfillmentId(fulfillmentMap.get(fulfillmentId).getId());
                                });
                            }
                        }
                    }
                }
            }
        }

    }
    private in.succinct.catalog.indexer.db.model.Payment ensurePayment(in.succinct.catalog.indexer.db.model.Provider provider, Payment bPayment) {
        in.succinct.catalog.indexer.db.model.Payment payment =  Database.getTable(in.succinct.catalog.indexer.db.model.Payment.class).newRecord();
        payment.setSubscriberId(provider.getSubscriberId());
        payment.setProviderId(provider.getId());
        payment.setObjectId(bPayment.getId());
        payment.setObjectName(bPayment.getType().toString());
        payment.setObjectJson(bPayment.toString());
        payment = Database.getTable(in.succinct.catalog.indexer.db.model.Payment.class).getRefreshed(payment);
        payment.save();
        return payment;
    }

    private <T extends Model & IndexedProviderModel>  T ensureProviderModel(Class<T> modelClass, in.succinct.catalog.indexer.db.model.Provider provider, boolean active, BecknObject becknObject){
        return ensureProviderModel(modelClass,provider,active,becknObject,null);
    }

    @SuppressWarnings("unchecked")
    private <T extends Model & IndexedProviderModel, B extends BecknObject>  T ensureProviderModel(Class<T> modelClass, in.succinct.catalog.indexer.db.model.Provider provider, boolean active,
                                                                                                   B becknObject, Visitor<T,B> visitor){
        T model =  Database.getTable(modelClass).newRecord();
        model.setSubscriberId(provider.getSubscriberId());
        model.setProviderId(provider.getId());
        model.setObjectId(becknObject.get("id"));
        Descriptor descriptor = becknObject.get(Descriptor.class,"descriptor");
        if (descriptor != null) {
            model.setObjectName(descriptor.getName());
        }
        if (model instanceof IndexedActivatableModel){
            ((IndexedActivatableModel)model).setActive(active);
        }
        if (visitor != null){
            visitor.visit(model,becknObject);
        }
        Config.instance().getLogger(getClass().getName()).info("CatalogDigester: model saved: " + model.getRawRecord().toString());
        model = Database.getTable(modelClass).getRefreshed(model);

        BecknObject finalObjectJson = new BecknObject();
        if (!ObjectUtil.isVoid(model.getObjectJson())){
            finalObjectJson = new BecknObject((JSONObject) BecknObject.parse(model.getObjectJson()));
        }
        finalObjectJson.getInner().putAll(becknObject.getInner()); //minimal
        model.setObjectJson(finalObjectJson.getInner().toString());
        model.save();
        return model;
    }

    private interface Visitor<M extends Model & IndexedProviderModel,B extends BecknObject> {
        void visit(M model , B becknObject);
    }

}
