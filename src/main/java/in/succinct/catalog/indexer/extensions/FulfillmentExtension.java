package in.succinct.catalog.indexer.extensions;

import com.venky.swf.db.extensions.ModelOperationExtension;
import in.succinct.beckn.BecknStrings;
import in.succinct.catalog.indexer.db.model.Fulfillment;

public class FulfillmentExtension extends ModelOperationExtension<Fulfillment> {
    static {
        registerExtension(new FulfillmentExtension());
    }

    @Override
    protected void afterCreate(Fulfillment instance) {
        super.afterCreate(instance);
        instance.getProvider().getItems().forEach((item)->{
            BecknStrings fIds  = BecknStrings.parse(item.getFulfillmentIds());
            if (!fIds.getInnerArray().contains(instance.getObjectId())){
                fIds.add(instance.getObjectId());
            }
            item.setFulfillmentIds(fIds.toString());
            item.save();
        });
    }
    @Override
    protected void afterDestroy(Fulfillment instance) {
        super.afterDestroy(instance);
        instance.getProvider().getItems().forEach((item)->{
            BecknStrings fIds  = BecknStrings.parse(item.getFulfillmentIds());
            if (fIds.getInnerArray().contains(instance.getObjectId())){
                fIds.remove(instance.getObjectId());
            }
            item.setFulfillmentIds(fIds.toString());
            item.save();
        });
    }

}
