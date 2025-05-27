package in.succinct.catalog.indexer.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.extensions.ModelOperationExtension;
import in.succinct.catalog.indexer.db.model.Item;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import in.succinct.onet.core.adaptor.NetworkAdaptor.Domain;
import in.succinct.onet.core.adaptor.NetworkAdaptorFactory;

public class ItemExtension extends ModelOperationExtension<Item> {
    static {
        registerExtension(new ItemExtension());
    }
    
    @Override
    protected void beforeValidate(Item instance) {
        super.beforeValidate(instance);
        
        if (ObjectUtil.isVoid(instance.getDomain())){
            throw new RuntimeException("Product domain is blank!!");
        }
        NetworkAdaptor networkAdaptor  = NetworkAdaptorFactory.getInstance().getAdaptor();
        boolean valid = false;
        for (Domain domain : networkAdaptor.getDomains()) {
            if (ObjectUtil.equals(domain.getId(),instance.getDomain())){
                valid = true ;
                break ;
            }
        }
        if (!valid){
            throw new RuntimeException("Invalid domain");
        }
        
    }
}
