package in.succinct.catalog.indexer.controller;

import com.venky.core.util.ObjectUtil;
import com.venky.swf.controller.ModelController;
import com.venky.swf.controller.annotations.RequireLogin;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.path.Path;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.plugins.background.core.TaskManager;
import com.venky.swf.views.View;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Context;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.catalog.indexer.ingest.CatalogDigester;
import in.succinct.catalog.indexer.ingest.CatalogDigester.Operation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ProvidersController extends ModelController<in.succinct.catalog.indexer.db.model.Provider> {
    @SuppressWarnings("unused")
    public ProvidersController(Path path) {
        super(path);
    }

    @RequireLogin(false)
    public View activate(){
        return update(true);
    }
    @RequireLogin(false)
    public View deactivate(){
        return update(false);
    }

    @RequireLogin(false)
    public  View update(boolean active){
        try {
            Request request = new Request((JSONObject) JSONValue.parseWithException(new InputStreamReader(getPath().getInputStream())));
            for (Provider provider : request.getMessage().getCatalog().getProviders()){
                provider.setTag("general_attributes","catalog.indexer.operation", active ? Operation.activate.name() : Operation.deactivate.name());
                provider.setTag("general_attributes","catalog.indexer.reset","N");
            }
            CatalogDigester digester = new CatalogDigester(request.getContext(),request.getMessage().getCatalog());
            TaskManager.instance().executeAsync(digester,false);
            return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);

        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }




    @RequireLogin(false)
    @SuppressWarnings("unused")
    public View ingest() throws Exception{
        ensureIntegrationMethod(HttpMethod.POST);
        Request request = new Request((JSONObject) JSONValue.parseWithException(new InputStreamReader(getPath().getInputStream())));
        CatalogDigester digester = new CatalogDigester(request.getContext(),request.getMessage().getCatalog());
        TaskManager.instance().executeAsync(digester,false);
        return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);
    }

}
