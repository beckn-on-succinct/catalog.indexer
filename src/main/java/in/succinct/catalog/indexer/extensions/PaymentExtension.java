package in.succinct.catalog.indexer.extensions;

import com.venky.swf.db.extensions.ModelOperationExtension;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Item;
import in.succinct.catalog.indexer.db.model.Payment;
import org.json.simple.JSONArray;

public class PaymentExtension extends ModelOperationExtension<Payment> {
    static {
        registerExtension( new PaymentExtension());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void afterCreate(Payment instance) {
        super.afterCreate(instance);
        instance.getProvider().getItems().forEach((item)->{
            JSONArray paymentIds  = BecknStrings.parse(item.getPaymentIds());

            if (!paymentIds.contains(instance.getObjectId())){
                paymentIds.add(instance.getObjectId());
                item.setPaymentIds(paymentIds.toString());

                Item becknItem = new Item(item.getObjectJson());
                becknItem.getPaymentIds().add(instance.getObjectId());
                item.setObjectJson(becknItem.getInner().toString());

                item.save();
            }
        });
    }

    @Override
    protected void afterDestroy(Payment instance) {
        super.afterDestroy(instance);
        instance.getProvider().getItems().forEach((item)->{
            JSONArray paymentIds  = BecknStrings.parse(item.getPaymentIds());
            boolean removed = paymentIds.remove(instance.getObjectId());
            if (removed) {
                item.setPaymentIds(paymentIds.toString());

                Item becknItem = new Item(item.getObjectJson());
                becknItem.getPaymentIds().add(instance.getObjectId());
                item.setObjectJson(becknItem.getInner().toString());
                item.save();
            }
        });
    }
}
