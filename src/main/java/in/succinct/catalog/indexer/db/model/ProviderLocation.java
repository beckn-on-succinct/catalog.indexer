package in.succinct.catalog.indexer.db.model;

import com.venky.geo.GeoLocation;
import com.venky.swf.db.annotations.column.IS_NULLABLE;
import com.venky.swf.db.model.Model;

import java.math.BigDecimal;

public interface ProviderLocation extends Model ,IndexedProviderModel , HasDescriptor , GeoLocation {
    @IS_NULLABLE
    public BigDecimal getMinLat();
    public void setMinLat(BigDecimal lat);

    @IS_NULLABLE
    public BigDecimal getMinLng();
    public void setMinLng(BigDecimal lat);

    @IS_NULLABLE
    public BigDecimal getMaxLat();
    public void setMaxLat(BigDecimal lat);

    @IS_NULLABLE
    public BigDecimal getMaxLng();
    public void setMaxLng(BigDecimal lng);

}
