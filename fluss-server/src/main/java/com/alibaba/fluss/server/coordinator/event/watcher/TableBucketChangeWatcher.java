package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.server.coordinator.event.AlterTableBucketEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdsZNode;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A watcher to watch the table bucket expansion in zookeeper. */
public class TableBucketChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TableBucketChangeWatcher.class);

    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;

    public TableBucketChangeWatcher(ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.curatorCache =
                CuratorCache.build(zooKeeperClient.getCuratorClient(), TableIdsZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new TableBucketChangeListener());
    }

    public void start() {
        running = true;
        curatorCache.start();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping TableBucketChangeWatcher");
        curatorCache.close();
    }

    private final class TableBucketChangeListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (newData != null) {
                LOG.debug("Received {} event (path: {})", type, newData.getPath());
            } else {
                LOG.debug("Received {} event", type);
            }

            switch (type) {
                case NODE_CHANGED:
                    {
                        if (newData != null) {
                            Long tableId = TableIdZNode.parsePath(newData.getPath());
                            if (tableId == null) {
                                break;
                            }
                            TableAssignment tableAssignment =
                                    TableIdZNode.decode(newData.getData());
                            eventManager.put(new AlterTableBucketEvent(tableId, tableAssignment));
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }
}
