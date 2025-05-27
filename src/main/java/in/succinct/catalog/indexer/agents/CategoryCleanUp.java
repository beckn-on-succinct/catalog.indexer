package in.succinct.catalog.indexer.agents;

import com.venky.swf.db.Database;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.plugins.background.core.agent.AgentSeederTask;
import com.venky.swf.plugins.background.core.agent.AgentSeederTaskBuilder;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Select;
import in.succinct.catalog.indexer.db.model.Category;

import java.util.ArrayList;
import java.util.List;

public class CategoryCleanUp extends AgentSeederTask implements AgentSeederTaskBuilder {
    @Override
    public List<Task> getTasks() {
        Select select = new Select().from(Category.class);
        select.add(" where  not exists (select 1 from items " +
                " where items.subscriber_id = categories.subscriber_id " +
                " and items.provider_id = categories.provider_id " +
                " and items.category_ids like concat( '%', categories.object_id , '%') " +
                " and position(categories.object_id in items.category_ids) > 0 ) "); // This extra. not really needed.
        List<Category> categories = select.execute();
        List<Task> tasks = new ArrayList<>();
        categories.forEach(c->{
            tasks.add(new CategoryDeleteTask(c.getId()));
        });
        return tasks;
    }
    
    
    
    @Override
    public String getAgentName() {
        return CATEGORY_CLEANUP;
    }
    
    @Override
    public AgentSeederTask createSeederTask() {
        return this;
    }
    
    public static final String CATEGORY_CLEANUP = "CATEGORY_CLEANUP";
    
    public static class CategoryDeleteTask implements Task{
        long categoryId = -1;
        public CategoryDeleteTask(long categoryId){
            this.categoryId = categoryId;
        }
        public CategoryDeleteTask(){
        
        }
        
        @Override
        public void execute() {
            if (categoryId > 0) {
                Category category = Database.getTable(Category.class).get(categoryId);
                Config.instance().getLogger(getClass().getName()).info("Deleting %d".formatted(categoryId));
                //category.destroy();
            }
        }
    }
}
