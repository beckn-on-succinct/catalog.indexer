package in.succinct.catalog.indexer.extensions;

import com.venky.swf.plugins.background.core.agent.Agent;
import in.succinct.catalog.indexer.agents.CategoryCleanUp;

public class AgentRegistry {
    static {
        Agent.instance().registerAgentSeederTaskBuilder(CategoryCleanUp.CATEGORY_CLEANUP,new CategoryCleanUp());
    }
}
