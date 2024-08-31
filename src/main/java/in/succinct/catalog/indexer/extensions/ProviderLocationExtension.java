package in.succinct.catalog.indexer.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.extensions.ModelOperationExtension;
import com.venky.swf.plugins.collab.util.BoundingBox;
import in.succinct.beckn.Circle;
import in.succinct.beckn.Location;
import in.succinct.beckn.Scalar;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.catalog.indexer.ingest.DistanceUtil;

public class ProviderLocationExtension extends ModelOperationExtension<ProviderLocation> {
    static {
        registerExtension(new ProviderLocationExtension());
    }

    @Override
    protected void beforeValidate(ProviderLocation instance) {
        super.beforeValidate(instance);
        Location location = new Location(instance.getObjectJson());
        GeoCoordinate gps = location.getGps();
        if (gps == null){
            return;
        }
        instance.setLat(gps.getLat());
        instance.setLng(gps.getLng());
        Circle circle = location.getCircle();
        if (circle == null){
            return;
        }
        Scalar scalar = circle.getRadius();
        if (scalar == null){
            return;
        }
        double radius = scalar.getValue();
        String unit  = scalar.getUnit();
        if (ObjectUtil.isVoid(unit)){
            scalar.setUnit("km");
        }else if (unit.equalsIgnoreCase("km")){
            radius = DistanceUtil.convertDistanceToKm(radius, scalar.getUnit());
        }
        BoundingBox boundingBox = new BoundingBox(gps,0,radius);
        GeoCoordinate min = boundingBox.getMin();
        GeoCoordinate max = boundingBox.getMax();
        instance.setMinLat(min.getLat());
        instance.setMinLng(min.getLng());
        instance.setMaxLat(max.getLat());
        instance.setMaxLng(max.getLng());
    }


}
