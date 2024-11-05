package in.succinct.catalog.indexer.extensions;

import com.venky.swf.db.extensions.ModelOperationExtension;
import in.succinct.beckn.BecknStrings;
import in.succinct.catalog.indexer.db.model.Payment;

public class PaymentExtension extends ModelOperationExtension<Payment> {
    static {
        registerExtension( new PaymentExtension());
    }

    @Override
    protected void afterCreate(Payment instance) {
        super.afterCreate(instance);
        instance.getProvider().getItems().forEach((item)->{
            BecknStrings paymentIds  = BecknStrings.parse(item.getPaymentIds());
            if (!paymentIds.getInnerArray().contains(instance.getObjectId())){
                paymentIds.add(instance.getObjectId());
            }
            item.setPaymentIds(paymentIds.toString());
            item.save();
        });
    }

    @Override
    protected void afterDestroy(Payment instance) {
        super.afterDestroy(instance);
        instance.getProvider().getItems().forEach((item)->{
            BecknStrings paymentIds  = BecknStrings.parse(item.getPaymentIds());
            if (paymentIds.getInnerArray().contains(instance.getObjectId())){
                paymentIds.remove(instance.getObjectId());
            }
            item.setPaymentIds(paymentIds.toString());
            item.save();
        });
    }
}
