package sample_team.module.algorithm;

import adf.core.agent.communication.MessageManager;
import adf.core.agent.develop.DevelopData;
import adf.core.agent.info.AgentInfo;
import adf.core.agent.info.ScenarioInfo;
import adf.core.agent.info.WorldInfo;
import adf.core.agent.module.ModuleManager;
import adf.core.agent.precompute.PrecomputeData;
import adf.core.component.module.algorithm.Clustering;
import adf.core.component.module.algorithm.StaticClustering;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import rescuecore2.misc.Pair;
import rescuecore2.misc.collections.LazyMap;
import rescuecore2.misc.geometry.Point2D;
import rescuecore2.standard.entities.Area;
import rescuecore2.standard.entities.Blockade;
import rescuecore2.standard.entities.Edge;
import rescuecore2.standard.entities.Human;
import rescuecore2.standard.entities.StandardEntity;
import rescuecore2.standard.entities.StandardEntityURN;
import rescuecore2.worldmodel.Entity;
import rescuecore2.worldmodel.EntityID;

import adf.core.debug.DefaultLogger;
import org.apache.log4j.Logger;

public class HashKMeansClustering extends StaticClustering {

  private static final String KEY_CLUSTER_SIZE = "clustering.size";
  private static final String KEY_CLUSTER_CENTER = "clustering.centers";
  private static final String KEY_CLUSTER_ENTITY = "clustering.entities.";
  private static final String KEY_ASSIGN_AGENT = "clustering.assign";

  private int repeatPrecompute;
  private int repeatPreparate;

  private Logger logger;

  private Collection<StandardEntity> entities;

  private List<StandardEntity> centerList;
  private List<EntityID> centerIDs;
  private Map<Integer, List<StandardEntity>> clusterEntitiesList;
  private List<List<EntityID>> clusterEntityIDsList;

  private int clusterSize;

  private boolean assignAgentsFlag;

  private Map<EntityID, Set<EntityID>> shortestPathGraph;
  
  // 最適化用のキャッシュとマップ
  private Map<EntityID, Integer> entityToClusterMap;
  private Map<StandardEntity, Integer> centerToIndexMap;
  private Map<String, Double> distanceCache;
  private Map<String, List<EntityID>> pathCache;

  public HashKMeansClustering(AgentInfo ai, WorldInfo wi, ScenarioInfo si, ModuleManager moduleManager, DevelopData developData) {
    super(ai, wi, si, moduleManager, developData);

    this.logger = DefaultLogger.getLogger(agentInfo.me());

    this.repeatPrecompute = developData.getInteger(
        "sample_team.module.algorithm.KMeansClustering.repeatPrecompute", 7);
    this.repeatPreparate = developData.getInteger(
        "sample_team.module.algorithm.KMeansClustering.repeatPreparate", 30);
    this.clusterSize = developData.getInteger(
        "sample_team.module.algorithm.KMeansClustering.clusterSize", 5);
    if (agentInfo.me().getStandardURN()
        .equals(StandardEntityURN.AMBULANCE_TEAM)) {
      this.clusterSize = scenarioInfo.getScenarioAgentsAt();
    } else if (agentInfo.me().getStandardURN()
        .equals(StandardEntityURN.FIRE_BRIGADE)) {
      this.clusterSize = scenarioInfo.getScenarioAgentsFb();
    } else if (agentInfo.me().getStandardURN()
        .equals(StandardEntityURN.POLICE_FORCE)) {
      this.clusterSize = scenarioInfo.getScenarioAgentsPf();
    }
    this.assignAgentsFlag = developData.getBoolean(
        "sample_team.module.algorithm.KMeansClustering.assignAgentsFlag", true);
    this.clusterEntityIDsList = new ArrayList<>();
    this.centerIDs = new ArrayList<>();
    this.clusterEntitiesList = new HashMap<>();
    this.centerList = new ArrayList<>();
    this.entities = wi.getEntitiesOfType(StandardEntityURN.ROAD,
        StandardEntityURN.HYDRANT, StandardEntityURN.BUILDING,
        StandardEntityURN.REFUGE, StandardEntityURN.GAS_STATION,
        StandardEntityURN.AMBULANCE_CENTRE, StandardEntityURN.FIRE_STATION,
        StandardEntityURN.POLICE_OFFICE);
    
    // 最適化用のデータ構造を初期化
    this.entityToClusterMap = new HashMap<>();
    this.centerToIndexMap = new HashMap<>();
    this.distanceCache = new HashMap<>();
    this.pathCache = new HashMap<>();
  }


  @Override
  public Clustering updateInfo(MessageManager messageManager) {
    super.updateInfo(messageManager);
    if (this.getCountUpdateInfo() >= 2) {
      return this;
    }
    this.centerList.clear();
    this.clusterEntitiesList.clear();
    this.centerToIndexMap.clear();
    this.distanceCache.clear();
    this.pathCache.clear();
    return this;
  }


  @Override
  public Clustering precompute(PrecomputeData precomputeData) {
    super.precompute(precomputeData);
    if (this.getCountPrecompute() >= 2) {
      return this;
    }
    this.calcPathBased(this.repeatPrecompute);
    this.entities = null;
    // write
    precomputeData.setInteger(KEY_CLUSTER_SIZE, this.clusterSize);
    precomputeData.setEntityIDList(KEY_CLUSTER_CENTER, this.centerIDs);
    for (int i = 0; i < this.clusterSize; i++) {
      precomputeData.setEntityIDList(KEY_CLUSTER_ENTITY + i,
          this.clusterEntityIDsList.get(i));
    }
    precomputeData.setBoolean(KEY_ASSIGN_AGENT, this.assignAgentsFlag);

    // --- ★ クラスタリング結果を標準出力に出す ---
    System.out.println("=== KMeansClustering Result ===");
    System.out.println("Cluster count: " + this.getClusterNumber());
    for (int i = 0; i < this.getClusterNumber(); i++) {
        Collection<EntityID> clusterEntities = this.getClusterEntityIDs(i);
        System.out.println(
            "[DEBUG] Cluster " + (i + 1)
            + " | Size: " + clusterEntities.size()
            + " | Entities: " + clusterEntities
        );
    }
    System.out.println("===============================");

    for(int i = 1; i < this.getClusterNumber() + 1; i++) {
        Collection<EntityID> myClusterEntities = this.getClusterEntityIDs(i-1);
        logger.debug("cluster="+i+", EntityID="+myClusterEntities.toString());
    }

    return this;
  }


  @Override
  public Clustering resume(PrecomputeData precomputeData) {
    super.resume(precomputeData);
    if (this.getCountResume() >= 2) {
      return this;
    }
    this.entities = null;
    // read
    this.clusterSize = precomputeData.getInteger(KEY_CLUSTER_SIZE);
    this.centerIDs = new ArrayList<>(
        precomputeData.getEntityIDList(KEY_CLUSTER_CENTER));
    this.clusterEntityIDsList = new ArrayList<>(this.clusterSize);
    for (int i = 0; i < this.clusterSize; i++) {
      this.clusterEntityIDsList.add(i,
          precomputeData.getEntityIDList(KEY_CLUSTER_ENTITY + i));
    }
    this.assignAgentsFlag = precomputeData.getBoolean(KEY_ASSIGN_AGENT);
    
    // 最適化用マップを更新
    this.updateOptimizationMaps();
    
    return this;
  }


  @Override
  public Clustering preparate() {
    super.preparate();
    if (this.getCountPreparate() >= 2) {
      return this;
    }
    this.calcStandard(this.repeatPreparate);
    this.entities = null;
    return this;
  }


  @Override
  public int getClusterNumber() {
    // The number of clusters
    return this.clusterSize;
  }


  @Override
  public int getClusterIndex(StandardEntity entity) {
    return this.getClusterIndex(entity.getID());
  }


  @Override
  public int getClusterIndex(EntityID id) {
    // 最適化: O(1)でクラスタインデックスを取得
    Integer clusterIndex = this.entityToClusterMap.get(id);
    if (clusterIndex != null) {
      return clusterIndex;
    }
    
    // フォールバック: マップが更新されていない場合
    for (int i = 0; i < this.clusterSize; i++) {
      if (this.clusterEntityIDsList.get(i).contains(id)) {
        this.entityToClusterMap.put(id, i);
        return i;
      }
    }
    return -1;
  }


  @Override
  public Collection<StandardEntity> getClusterEntities(int index) {
    List<StandardEntity> result = this.clusterEntitiesList.get(index);
    if (result == null || result.isEmpty()) {
      List<EntityID> list = this.clusterEntityIDsList.get(index);
      result = new ArrayList<>(list.size());
      for (int i = 0; i < list.size(); i++) {
        result.add(i, this.worldInfo.getEntity(list.get(i)));
      }
      this.clusterEntitiesList.put(index, result);
    }
    return result;
  }


  @Override
  public Collection<EntityID> getClusterEntityIDs(int index) {
    return this.clusterEntityIDsList.get(index);
  }


  @Override
  public Clustering calc() {
    return this;
  }


  private void calcStandard(int repeat) {
    this.initShortestPath(this.worldInfo);
    Random random = new Random();

    List<StandardEntity> entityList = new ArrayList<>(this.entities);
    this.centerList = new ArrayList<>(this.clusterSize);
    this.clusterEntitiesList = new HashMap<>(this.clusterSize);

    // init list
    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
      this.centerList.add(index, entityList.get(0));
    }
    System.out.println("[" + this.getClass().getSimpleName() + "] Cluster : "
        + this.clusterSize);
    
    // init center - 最適化: Set使用で重複チェックを高速化
    Set<StandardEntity> usedCenters = new HashSet<>();
    for (int index = 0; index < this.clusterSize; index++) {
      StandardEntity centerEntity;
      do {
        centerEntity = entityList
            .get(Math.abs(random.nextInt()) % entityList.size());
      } while (usedCenters.contains(centerEntity));
      this.centerList.set(index, centerEntity);
      usedCenters.add(centerEntity);
    }
    
    // centerToIndexMapを構築
    this.updateCenterToIndexMap();
    
    // calc center
    for (int i = 0; i < repeat; i++) {
      this.clusterEntitiesList.clear();
      for (int index = 0; index < this.clusterSize; index++) {
        this.clusterEntitiesList.put(index, new ArrayList<>());
      }
      for (StandardEntity entity : entityList) {
        StandardEntity tmp = this.getNearEntityByLine(this.worldInfo,
            this.centerList, entity);
        Integer clusterIndex = this.centerToIndexMap.get(tmp);
        if (clusterIndex != null) {
          this.clusterEntitiesList.get(clusterIndex).add(entity);
        }
      }
      
      boolean centersChanged = false;
      for (int index = 0; index < this.clusterSize; index++) {
        List<StandardEntity> clusterEntities = this.clusterEntitiesList.get(index);
        if (clusterEntities.isEmpty()) continue;
        
        int sumX = 0, sumY = 0;
        for (StandardEntity entity : clusterEntities) {
          Pair<Integer, Integer> location = this.worldInfo.getLocation(entity);
          sumX += location.first();
          sumY += location.second();
        }
        int centerX = sumX / clusterEntities.size();
        int centerY = sumY / clusterEntities.size();
        StandardEntity center = this.getNearEntityByLine(this.worldInfo,
            clusterEntities, centerX, centerY);
        
        StandardEntity newCenter = center;
        if (center instanceof Area) {
          newCenter = center;
        } else if (center instanceof Human) {
          newCenter = this.worldInfo.getEntity(((Human) center).getPosition());
        } else if (center instanceof Blockade) {
          newCenter = this.worldInfo.getEntity(((Blockade) center).getPosition());
        }
        
        if (!newCenter.equals(this.centerList.get(index))) {
          this.centerList.set(index, newCenter);
          centersChanged = true;
        }
      }
      
      if (centersChanged) {
        this.updateCenterToIndexMap();
      }
      
      if (scenarioInfo.isDebugMode()) {
        System.out.print("*");
      }
    }

    if (scenarioInfo.isDebugMode()) {
      System.out.println();
    }

    // set entity
    this.clusterEntitiesList.clear();
    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
    }
    for (StandardEntity entity : entityList) {
      StandardEntity tmp = this.getNearEntityByLine(this.worldInfo,
          this.centerList, entity);
      Integer clusterIndex = this.centerToIndexMap.get(tmp);
      if (clusterIndex != null) {
        this.clusterEntitiesList.get(clusterIndex).add(entity);
      }
    }

    // this.clusterEntitiesList.sort(comparing(List::size, reverseOrder()));

    if (this.assignAgentsFlag) {
      List<StandardEntity> firebrigadeList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.FIRE_BRIGADE));
      List<StandardEntity> policeforceList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.POLICE_FORCE));
      List<StandardEntity> ambulanceteamList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.AMBULANCE_TEAM));

      this.assignAgents(this.worldInfo, firebrigadeList);
      this.assignAgents(this.worldInfo, policeforceList);
      this.assignAgents(this.worldInfo, ambulanceteamList);
    }

    this.centerIDs = new ArrayList<>();
    for (int i = 0; i < this.centerList.size(); i++) {
      this.centerIDs.add(i, this.centerList.get(i).getID());
    }
    for (int index = 0; index < this.clusterSize; index++) {
      List<StandardEntity> entities = this.clusterEntitiesList.get(index);
      List<EntityID> list = new ArrayList<>(entities.size());
      for (int i = 0; i < entities.size(); i++) {
        list.add(i, entities.get(i).getID());
      }
      this.clusterEntityIDsList.add(index, list);
    }
    
    // 最適化用マップを更新
    this.updateOptimizationMaps();
  }


  private void calcPathBased(int repeat) {
    this.initShortestPath(this.worldInfo);
    Random random = new Random();
    List<StandardEntity> entityList = new ArrayList<>(this.entities);
    this.centerList = new ArrayList<>(this.clusterSize);
    this.clusterEntitiesList = new HashMap<>(this.clusterSize);

    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
      this.centerList.add(index, entityList.get(0));
    }
    
    // init center - 最適化: Set使用で重複チェックを高速化
    Set<StandardEntity> usedCenters = new HashSet<>();
    for (int index = 0; index < this.clusterSize; index++) {
      StandardEntity centerEntity;
      do {
        centerEntity = entityList
            .get(Math.abs(random.nextInt()) % entityList.size());
      } while (usedCenters.contains(centerEntity));
      this.centerList.set(index, centerEntity);
      usedCenters.add(centerEntity);
    }
    
    // centerToIndexMapを構築
    this.updateCenterToIndexMap();
    
    for (int i = 0; i < repeat; i++) {
      this.clusterEntitiesList.clear();
      for (int index = 0; index < this.clusterSize; index++) {
        this.clusterEntitiesList.put(index, new ArrayList<>());
      }
      for (StandardEntity entity : entityList) {
        StandardEntity tmp = this.getNearEntity(this.worldInfo, this.centerList,
            entity);
        Integer clusterIndex = this.centerToIndexMap.get(tmp);
        if (clusterIndex != null) {
          this.clusterEntitiesList.get(clusterIndex).add(entity);
        }
      }
      
      boolean centersChanged = false;
      for (int index = 0; index < this.clusterSize; index++) {
        List<StandardEntity> clusterEntities = this.clusterEntitiesList.get(index);
        if (clusterEntities.isEmpty()) continue;
        
        int sumX = 0, sumY = 0;
        for (StandardEntity entity : clusterEntities) {
          Pair<Integer, Integer> location = this.worldInfo.getLocation(entity);
          sumX += location.first();
          sumY += location.second();
        }
        int centerX = sumX / clusterEntities.size();
        int centerY = sumY / clusterEntities.size();

        // this.centerList.set(index, getNearEntity(this.worldInfo,
        // this.clusterEntitiesList.get(index), centerX, centerY));
        StandardEntity center = this.getNearEntity(this.worldInfo,
            clusterEntities, centerX, centerY);
        
        StandardEntity newCenter = center;
        if (center instanceof Area) {
          newCenter = center;
        } else if (center instanceof Human) {
          newCenter = this.worldInfo.getEntity(((Human) center).getPosition());
        } else if (center instanceof Blockade) {
          newCenter = this.worldInfo.getEntity(((Blockade) center).getPosition());
        }
        
        if (!newCenter.equals(this.centerList.get(index))) {
          this.centerList.set(index, newCenter);
          centersChanged = true;
        }
      }
      
      if (centersChanged) {
        this.updateCenterToIndexMap();
      }
      
      if (scenarioInfo.isDebugMode()) {
        System.out.print("*");
      }
    }

    if (scenarioInfo.isDebugMode()) {
      System.out.println();
    }

    this.clusterEntitiesList.clear();
    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
    }
    for (StandardEntity entity : entityList) {
      StandardEntity tmp = this.getNearEntity(this.worldInfo, this.centerList,
          entity);
      Integer clusterIndex = this.centerToIndexMap.get(tmp);
      if (clusterIndex != null) {
        this.clusterEntitiesList.get(clusterIndex).add(entity);
      }
    }
    // this.clusterEntitiesList.sort(comparing(List::size, reverseOrder()));
    if (this.assignAgentsFlag) {
      List<StandardEntity> fireBrigadeList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.FIRE_BRIGADE));
      List<StandardEntity> policeForceList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.POLICE_FORCE));
      List<StandardEntity> ambulanceTeamList = new ArrayList<>(
          this.worldInfo.getEntitiesOfType(StandardEntityURN.AMBULANCE_TEAM));
      this.assignAgents(this.worldInfo, fireBrigadeList);
      this.assignAgents(this.worldInfo, policeForceList);
      this.assignAgents(this.worldInfo, ambulanceTeamList);
    }

    this.centerIDs = new ArrayList<>();
    for (int i = 0; i < this.centerList.size(); i++) {
      this.centerIDs.add(i, this.centerList.get(i).getID());
    }
    for (int index = 0; index < this.clusterSize; index++) {
      List<StandardEntity> entities = this.clusterEntitiesList.get(index);
      List<EntityID> list = new ArrayList<>(entities.size());
      for (int i = 0; i < entities.size(); i++) {
        list.add(i, entities.get(i).getID());
      }
      this.clusterEntityIDsList.add(index, list);
    }
    
    // 最適化用マップを更新
    this.updateOptimizationMaps();
  }


  private void assignAgents(WorldInfo world, List<StandardEntity> agentList) {
    int clusterIndex = 0;
    while (agentList.size() > 0) {
      StandardEntity center = this.centerList.get(clusterIndex);
      StandardEntity agent = this.getNearAgent(world, agentList, center);
      this.clusterEntitiesList.get(clusterIndex).add(agent);
      agentList.remove(agent);
      clusterIndex++;
      if (clusterIndex >= this.clusterSize) {
        clusterIndex = 0;
      }
    }
  }


  private StandardEntity getNearEntityByLine(WorldInfo world,
      List<StandardEntity> srcEntityList, StandardEntity targetEntity) {
    Pair<Integer, Integer> location = world.getLocation(targetEntity);
    return this.getNearEntityByLine(world, srcEntityList, location.first(),
        location.second());
  }


  private StandardEntity getNearEntityByLine(WorldInfo world,
      List<StandardEntity> srcEntityList, int targetX, int targetY) {
    StandardEntity result = null;
    for (StandardEntity entity : srcEntityList) {
      result = ((result != null)
          ? this.compareLineDistance(world, targetX, targetY, result, entity)
          : entity);
    }
    return result;
  }


  private StandardEntity getNearAgent(WorldInfo worldInfo,
      List<StandardEntity> srcAgentList, StandardEntity targetEntity) {
    StandardEntity result = null;
    for (StandardEntity agent : srcAgentList) {
      Human human = (Human) agent;
      if (result == null) {
        result = agent;
      } else {
        if (this
            .comparePathDistance(worldInfo, targetEntity, result,
                worldInfo.getPosition(human))
            .equals(worldInfo.getPosition(human))) {
          result = agent;
        }
      }
    }
    return result;
  }


  private StandardEntity getNearEntity(WorldInfo worldInfo,
      List<StandardEntity> srcEntityList, int targetX, int targetY) {
    StandardEntity result = null;
    for (StandardEntity entity : srcEntityList) {
      result = (result != null)
          ? this.compareLineDistance(worldInfo, targetX, targetY, result,
              entity)
          : entity;
    }
    return result;
  }


  private Point2D getEdgePoint(Edge edge) {
    Point2D start = edge.getStart();
    Point2D end = edge.getEnd();
    return new Point2D(((start.getX() + end.getX()) / 2.0D),
        ((start.getY() + end.getY()) / 2.0D));
  }


  private double getDistance(double fromX, double fromY, double toX,
      double toY) {
    double dx = fromX - toX;
    double dy = fromY - toY;
    return Math.hypot(dx, dy);
  }


  private double getDistance(Pair<Integer, Integer> from, Point2D to) {
    return getDistance(from.first(), from.second(), to.getX(), to.getY());
  }


  private double getDistance(Pair<Integer, Integer> from, Edge to) {
    return getDistance(from, getEdgePoint(to));
  }


  private double getDistance(Point2D from, Point2D to) {
    return getDistance(from.getX(), from.getY(), to.getX(), to.getY());
  }


  private double getDistance(Edge from, Edge to) {
    return getDistance(getEdgePoint(from), getEdgePoint(to));
  }


  private StandardEntity compareLineDistance(WorldInfo worldInfo, int targetX,
      int targetY, StandardEntity first, StandardEntity second) {
    Pair<Integer, Integer> firstLocation = worldInfo.getLocation(first);
    Pair<Integer, Integer> secondLocation = worldInfo.getLocation(second);
    double firstDistance = getDistance(firstLocation.first(),
        firstLocation.second(), targetX, targetY);
    double secondDistance = getDistance(secondLocation.first(),
        secondLocation.second(), targetX, targetY);
    return (firstDistance < secondDistance ? first : second);
  }


  private StandardEntity getNearEntity(WorldInfo worldInfo,
      List<StandardEntity> srcEntityList, StandardEntity targetEntity) {
    StandardEntity result = null;
    for (StandardEntity entity : srcEntityList) {
      result = (result != null)
          ? this.comparePathDistance(worldInfo, targetEntity, result, entity)
          : entity;
    }
    return result;
  }


  private StandardEntity comparePathDistance(WorldInfo worldInfo,
      StandardEntity target, StandardEntity first, StandardEntity second) {
    // キャッシュキーを生成
    String firstKey = target.getID() + "-" + first.getID();
    String secondKey = target.getID() + "-" + second.getID();
    
    Double firstDistance = this.distanceCache.get(firstKey);
    if (firstDistance == null) {
      firstDistance = getPathDistance(worldInfo,
          shortestPath(target.getID(), first.getID()));
      this.distanceCache.put(firstKey, firstDistance);
    }
    
    Double secondDistance = this.distanceCache.get(secondKey);
    if (secondDistance == null) {
      secondDistance = getPathDistance(worldInfo,
          shortestPath(target.getID(), second.getID()));
      this.distanceCache.put(secondKey, secondDistance);
    }
    
    return (firstDistance < secondDistance ? first : second);
  }


  private double getPathDistance(WorldInfo worldInfo, List<EntityID> path) {
    if (path == null)
      return Double.MAX_VALUE;
    if (path.size() <= 1)
      return 0.0D;

    double distance = 0.0D;
    int limit = path.size() - 1;

    Area area = (Area) worldInfo.getEntity(path.get(0));
    distance += getDistance(worldInfo.getLocation(area),
        area.getEdgeTo(path.get(1)));
    area = (Area) worldInfo.getEntity(path.get(limit));
    distance += getDistance(worldInfo.getLocation(area),
        area.getEdgeTo(path.get(limit - 1)));

    for (int i = 1; i < limit; i++) {
      area = (Area) worldInfo.getEntity(path.get(i));
      distance += getDistance(area.getEdgeTo(path.get(i - 1)),
          area.getEdgeTo(path.get(i + 1)));
    }
    return distance;
  }


  private void initShortestPath(WorldInfo worldInfo) {
    Map<EntityID,
        Set<EntityID>> neighbours = new LazyMap<EntityID, Set<EntityID>>() {

          @Override
          public Set<EntityID> createValue() {
            return new HashSet<>();
          }
        };
    for (Entity next : worldInfo) {
      if (next instanceof Area) {
        Collection<EntityID> areaNeighbours = ((Area) next).getNeighbours();
        neighbours.get(next.getID()).addAll(areaNeighbours);
      }
    }
    for (Map.Entry<EntityID, Set<EntityID>> graph : neighbours.entrySet()) {// fix
                                                                            // graph
      for (EntityID entityID : graph.getValue()) {
        neighbours.get(entityID).add(graph.getKey());
      }
    }
    this.shortestPathGraph = neighbours;
  }


  private List<EntityID> shortestPath(EntityID start, EntityID... goals) {
    return shortestPath(start, Arrays.asList(goals));
  }


  private List<EntityID> shortestPath(EntityID start,
      Collection<EntityID> goals) {
    // キャッシュキーを生成（start + goalsのハッシュ）
    String cacheKey = start.toString() + "-" + goals.hashCode();
    List<EntityID> cachedPath = this.pathCache.get(cacheKey);
    if (cachedPath != null) {
      return cachedPath;
    }
    
    List<EntityID> open = new LinkedList<>();
    Map<EntityID, EntityID> ancestors = new HashMap<>();
    open.add(start);
    EntityID next;
    boolean found = false;
    ancestors.put(start, start);
    do {
      next = open.remove(0);
      if (isGoal(next, goals)) {
        found = true;
        break;
      }
      Collection<EntityID> neighbours = shortestPathGraph.get(next);
      if (neighbours.isEmpty())
        continue;

      for (EntityID neighbour : neighbours) {
        if (isGoal(neighbour, goals)) {
          ancestors.put(neighbour, next);
          next = neighbour;
          found = true;
          break;
        } else if (!ancestors.containsKey(neighbour)) {
          open.add(neighbour);
          ancestors.put(neighbour, next);
        }
      }
    } while (!found && !open.isEmpty());
    if (!found) {
      // No path
      this.pathCache.put(cacheKey, null);
      return null;
    }
    // Walk back from goal to start
    EntityID current = next;
    List<EntityID> path = new LinkedList<>();
    do {
      path.add(0, current);
      current = ancestors.get(current);
      if (current == null)
        throw new RuntimeException(
            "Found a node with no ancestor! Something is broken.");
    } while (current != start);
    
    // 結果をキャッシュに保存
    this.pathCache.put(cacheKey, path);
    return path;
  }


  private boolean isGoal(EntityID e, Collection<EntityID> test) {
    return test.contains(e);
  }
  
  // 最適化用のヘルパーメソッド
  
  /**
   * centerListからcenterToIndexMapを構築する
   */
  private void updateCenterToIndexMap() {
    this.centerToIndexMap.clear();
    for (int i = 0; i < this.centerList.size(); i++) {
      this.centerToIndexMap.put(this.centerList.get(i), i);
    }
  }
  
  /**
   * 全ての最適化用マップを更新する
   */
  private void updateOptimizationMaps() {
    this.updateCenterToIndexMap();
    this.updateEntityToClusterMap();
  }
  
  /**
   * entityToClusterMapを構築する
   */
  private void updateEntityToClusterMap() {
    this.entityToClusterMap.clear();
    for (int i = 0; i < this.clusterEntityIDsList.size(); i++) {
      List<EntityID> entities = this.clusterEntityIDsList.get(i);
      if (entities != null) {
        for (EntityID entityID : entities) {
          this.entityToClusterMap.put(entityID, i);
        }
      }
    }
  }
}
