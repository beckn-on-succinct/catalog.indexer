package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.model.Model;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.routing.Config;
import in.succinct.beckn.BecknObject;
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
import in.succinct.beckn.TagGroup;
import in.succinct.beckn.TagGroupHolder;
import in.succinct.catalog.indexer.db.model.HasDescriptor;
import in.succinct.catalog.indexer.db.model.IndexedActivatableModel;
import in.succinct.catalog.indexer.db.model.IndexedProviderModel;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.catalog.indexer.db.model.ProviderTag;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class CatalogDigester implements Task {
    public enum Operation {
        delete,
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


            if (reset) {
                destroy(provider);
            }
            if (!reset || !ObjectUtil.isVoid(provider.getDescriptor().getName())) {
                ensureProvider(provider);
            }
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
                        JSONObject o = new JSONObject();
                        o.put("id",id);
                        if (c instanceof HasDescriptor){
                            Descriptor descriptor = new Descriptor();
                            descriptor.setName(id);
                            descriptor.setCode(id);
                            descriptor.setShortDesc(id);
                            descriptor.setLongDesc(id);
                            o.put("descriptor",descriptor.getInner());
                        }
                        c.setObjectJson(o.toString());
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

    @SuppressWarnings("unchecked")
    public void ensureProvider(Provider bProvider){

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
        provider.getProviderTags().forEach(providerTag -> providerTag.destroy()); //Tags to be reset.
        for (TagGroup group : bProvider.getTags()){
            for (TagGroup tag : group.getList()){
                ProviderTag providerTag = Database.getTable(ProviderTag.class).newRecord();
                providerTag.setProviderId(provider.getId());
                providerTag.setTagGroupCode(group.getId());
                providerTag.setTagCode(tag.getId());
                providerTag.setTagValue(tag.getValue());
                providerTag = Database.getTable(ProviderTag.class).getRefreshed(providerTag);
                providerTag.save();
            }
        }

        //Map<String, in.succinct.catalog.indexer.db.model.Item> itemMap = createDbCache(in.succinct.catalog.indexer.db.model.Item.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Category> categoryMap = createDbCache(in.succinct.catalog.indexer.db.model.Category.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Fulfillment> fulfillmentMap = createDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.Payment> paymentMap = createDbCache(in.succinct.catalog.indexer.db.model.Payment.class,provider);
        Map<String, in.succinct.catalog.indexer.db.model.ProviderLocation> providerLocationMap = createDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class,provider);



        if (categories != null){
            for (int j = 0 ; j < categories.size() ; j++){
                Category category = categories.get(j);
                in.succinct.catalog.indexer.db.model.Category model = ensureProviderModel(in.succinct.catalog.indexer.db.model.Category.class,provider,category,null);
                categoryMap.put(model.getObjectId(),model);
            }
        }
        if (fulfillments != null) {
            for (int j = 0; j < fulfillments.size(); j++) {
                Fulfillment fulfillment = fulfillments.get(j);
                in.succinct.catalog.indexer.db.model.Fulfillment model = ensureProviderModel(in.succinct.catalog.indexer.db.model.Fulfillment.class, provider,  fulfillment,(fulfillmentModel, fulfillmentBecknObject) -> fulfillmentModel.setObjectName(fulfillmentBecknObject.getType()));
                fulfillmentMap.put(model.getObjectId(), model);
            }
            provider.getFulfillments().forEach(f->{
                if (!fulfillmentMap.containsKey(f.getObjectId())){
                    f.destroy(); // Fulfillments are always sent completely.
                }
            });
        }
        if (payments != null) {
            for (int j = 0; j < payments.size(); j++) {
                Payment payment = payments.get(j);
                in.succinct.catalog.indexer.db.model.Payment model = ensurePayment(provider, payment);
                paymentMap.put(model.getObjectId(), model);
            }
            provider.getPayments().forEach(p->{
                if (!paymentMap.containsKey(p.getObjectId())){
                    p.destroy();// Payments are always sent completely. so missing ones have to be deleted.
                }
            });
        }
        if (locations != null) {
            for (int j = 0; j < locations.size(); j++) {
                Location location = locations.get(j);
                ProviderLocation model = ensureProviderModel(ProviderLocation.class, provider, location,(pl, l) -> pl.setObjectName(l.getDescriptor().getName()));
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
                item.getFulfillmentIds().getInner().sort((s1,s2)->((String)s1).compareTo((String)s2));
                if (item.getLocationIds() == null){
                    item.setLocationIds(new BecknStrings());
                    item.getLocationIds().add(item.getLocationId());
                }
                item.getLocationIds().getInner().sort((s1,s2)->((String)s1).compareTo((String)s2));

                if (item.getPaymentIds() == null){
                    item.setPaymentIds(new BecknStrings());
                    item.getPaymentIds().add(null);
                }
                item.getPaymentIds().getInner().sort((s1,s2)->((String)s1).compareTo((String)s2));
                if (item.getCategoryIds() == null){
                    item.setCategoryIds(new BecknStrings());
                    item.getCategoryIds().add(item.getCategoryId());
                }
                item.getCategoryIds().getInner().sort((s1,s2)->((String)s1).compareTo((String)s2));


                ensureProviderModel(in.succinct.catalog.indexer.db.model.Item.class, provider,
                        item, (model, becknObject) -> {
                    model.setCategoryIds(item.getCategoryIds().toString());
                    model.setLocationIds(item.getLocationIds().toString());
                    model.setPaymentIds(item.getPaymentIds().toString());
                    model.setFulfillmentIds(item.getFulfillmentIds().toString());
                    String itemDomain = item.getTag("domain","id");
                    if (ObjectUtil.isVoid(itemDomain)){
                        itemDomain = context.getDomain();
                    }
                    model.setDomain(itemDomain);
                });

            }
        }

    }
    private in.succinct.catalog.indexer.db.model.Payment ensurePayment(in.succinct.catalog.indexer.db.model.Provider provider, Payment bPayment) {
        in.succinct.catalog.indexer.db.model.Payment payment =  Database.getTable(in.succinct.catalog.indexer.db.model.Payment.class).newRecord();
        payment.setSubscriberId(provider.getSubscriberId());
        payment.setProviderId(provider.getId());
        payment.setObjectId(bPayment.getId());
        payment.setObjectName(bPayment.getInvoiceEvent().toString());
        payment.setObjectJson(bPayment.toString());
        payment = Database.getTable(in.succinct.catalog.indexer.db.model.Payment.class).getRefreshed(payment);
        payment.save();
        return payment;
    }


    @SuppressWarnings("unchecked")
    private <T extends Model & IndexedProviderModel, B extends BecknObject>  T ensureProviderModel(Class<T> modelClass, in.succinct.catalog.indexer.db.model.Provider provider,
                                                                                                   B becknObject, Visitor<T,B> visitor){
        T model =  Database.getTable(modelClass).newRecord();
        model.setSubscriberId(provider.getSubscriberId());
        model.setProviderId(provider.getId());
        model.setObjectId(becknObject.get("id"));
        Descriptor descriptor = becknObject.get(Descriptor.class,"descriptor");
        if (descriptor != null) {
            model.setObjectName(descriptor.getName());
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

        Operation operation = Operation.activate;

        if (becknObject instanceof TagGroupHolder){
            TagGroupHolder tagGroupHolder = (TagGroupHolder) becknObject;
            String op = tagGroupHolder.getTag("general_attributes","catalog.indexer.operation");
            if (!ObjectUtil.isVoid(op)){
                operation = Operation.valueOf(op);
            }
        }
        if (model instanceof IndexedActivatableModel) {
            if (operation == Operation.activate) {
                ((IndexedActivatableModel) model).setActive(true);
            } else if (operation == Operation.deactivate) {
                ((IndexedActivatableModel) model).setActive(false);
            }
        }

        if (operation == Operation.delete){
            model.destroy();
        }else {
            model.save();
        }
        return model;
    }

    private interface Visitor<M extends Model & IndexedProviderModel,B extends BecknObject> {
        void visit(M model , B becknObject);
    }

}
