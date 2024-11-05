package in.succinct.catalog.indexer.extensions;

import com.venky.swf.db.extensions.ModelOperationExtension;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Item;
import in.succinct.catalog.indexer.db.model.Fulfillment;
import in.succinct.json.JSONAwareWrapper;
import org.json.simple.JSONArray;

public class FulfillmentExtension extends ModelOperationExtension<Fulfillment> {
    static {
        registerExtension(new FulfillmentExtension());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void afterCreate(Fulfillment instance) {
        super.afterCreate(instance);
        instance.getProvider().getItems().forEach((item)->{
            JSONArray fIds  = BecknStrings.parse(item.getFulfillmentIds());
            if (!fIds.contains(instance.getObjectId())){
                fIds.add(instance.getObjectId());
                item.setFulfillmentIds(fIds.toString());

                Item becknItem = new Item(item.getObjectJson());
                becknItem.getFulfillmentIds().add(instance.getObjectId());
                item.setObjectJson(becknItem.getInner().toString());

                item.save();
            }
        });
    }
    @Override
    protected void afterDestroy(Fulfillment instance) {
        super.afterDestroy(instance);
        instance.getProvider().getItems().forEach((item)->{
            JSONArray fIds  = JSONAwareWrapper.parse(item.getFulfillmentIds());
            boolean removed = fIds.remove(instance.getObjectId());
            if (removed) {
                item.setFulfillmentIds(fIds.toString());

                Item becknItem = new Item(item.getObjectJson());
                becknItem.getFulfillmentIds().remove(instance.getObjectId());
                item.setObjectJson(becknItem.getInner().toString());

                item.save();
            }
        });
    }

}
