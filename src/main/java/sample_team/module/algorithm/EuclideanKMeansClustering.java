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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import rescuecore2.misc.Pair;
import rescuecore2.standard.entities.Area;
import rescuecore2.standard.entities.Blockade;
import rescuecore2.standard.entities.Human;
import rescuecore2.standard.entities.StandardEntity;
import rescuecore2.standard.entities.StandardEntityURN;
import rescuecore2.worldmodel.EntityID;

import adf.core.debug.DefaultLogger;
import org.apache.log4j.Logger;

public class EuclideanKMeansClustering extends StaticClustering {

  private static final String KEY_CLUSTER_SIZE = "clustering.size";
  private static final String KEY_CLUSTER_CENTER = "clustering.centers";
  private static final String KEY_CLUSTER_ENTITY = "clustering.entities.";
  private static final String KEY_ASSIGN_AGENT = "clustering.assign";

  // 高速化された反復回数
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

  // 高速化用のデータ構造
  private Map<EntityID, Integer> entityToClusterMap;
  private Map<StandardEntity, Integer> centerToIndexMap;
  
  // 座標配列（高速アクセス用）
  private double[] entityX;
  private double[] entityY;
  private Map<EntityID, Integer> entityIndexMap;

  public EuclideanKMeansClustering(AgentInfo ai, WorldInfo wi, ScenarioInfo si, ModuleManager moduleManager, DevelopData developData) {
    super(ai, wi, si, moduleManager, developData);

    this.logger = DefaultLogger.getLogger(agentInfo.me());
    
    // 反復回数を大幅削減
    this.repeatPrecompute = developData.getInteger(
        "sample_team.module.algorithm.KMeansClustering.repeatPrecompute", 3);
    this.repeatPreparate = developData.getInteger(
        "sample_team.module.algorithm.KMeansClustering.repeatPreparate", 5);

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
    
    // 高速化用のデータ構造を初期化
    this.entityToClusterMap = new HashMap<>();
    this.centerToIndexMap = new HashMap<>();
    this.entityIndexMap = new HashMap<>();
    
    // 座標配列を事前計算
    this.precomputeCoordinates();
  }

  /**
   * 座標配列を事前計算（高速アクセス用）
   */
  private void precomputeCoordinates() {
    List<StandardEntity> entityList = new ArrayList<>(this.entities);
    int entityCount = entityList.size();
    
    this.entityX = new double[entityCount];
    this.entityY = new double[entityCount];
    
    for (int i = 0; i < entityCount; i++) {
      StandardEntity entity = entityList.get(i);
      Pair<Integer, Integer> location = this.worldInfo.getLocation(entity);
      this.entityX[i] = location.first().doubleValue();
      this.entityY[i] = location.second().doubleValue();
      this.entityIndexMap.put(entity.getID(), i);
    }

    System.out.println("[KMeansClustering] Precomputed coordinates for " + entityCount + " entities");
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
    return this;
  }

  @Override
  public Clustering precompute(PrecomputeData precomputeData) {
    super.precompute(precomputeData);
    if (this.getCountPrecompute() >= 2) {
      return this;
    }
    
    // 高速ユークリッド距離ベースのクラスタリング実行
    this.calcFastEuclidean(this.repeatPrecompute);
    this.entities = null;
    
    // write
    precomputeData.setInteger(KEY_CLUSTER_SIZE, this.clusterSize);
    precomputeData.setEntityIDList(KEY_CLUSTER_CENTER, this.centerIDs);
    for (int i = 0; i < this.clusterSize; i++) {
      precomputeData.setEntityIDList(KEY_CLUSTER_ENTITY + i,
          this.clusterEntityIDsList.get(i));
    }
    precomputeData.setBoolean(KEY_ASSIGN_AGENT, this.assignAgentsFlag);

    // クラスタリング結果を出力
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
    System.out.println("=======================================");

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
    this.calcFastEuclidean(this.repeatPreparate);
    this.entities = null;
    return this;
  }

  @Override
  public int getClusterNumber() {
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

  /**
   * 高速ユークリッド距離ベースのKMeansクラスタリング
   * KmeansPPClusteringの高速技術を採用
   */
  private void calcFastEuclidean(int repeat) {
    Random random = new Random();
    List<StandardEntity> entityList = new ArrayList<>(this.entities);
    this.centerList = new ArrayList<>(this.clusterSize);
    this.clusterEntitiesList = new HashMap<>(this.clusterSize);

    // 初期化
    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
      this.centerList.add(index, entityList.get(0));
    }

    System.out.println("[KMeansClustering] Cluster size: " + this.clusterSize);

    // 初期センター選択（重複なし高速化）
    Set<StandardEntity> usedCenters = new HashSet<>();
    for (int index = 0; index < this.clusterSize; index++) {
      StandardEntity centerEntity;
      do {
        centerEntity = entityList.get(Math.abs(random.nextInt()) % entityList.size());
      } while (usedCenters.contains(centerEntity));
      this.centerList.set(index, centerEntity);
      usedCenters.add(centerEntity);
    }
    
    // centerToIndexMapを構築
    this.updateCenterToIndexMap();
    
    // メインのクラスタリングループ
    for (int iteration = 0; iteration < repeat; iteration++) {
      this.clusterEntitiesList.clear();
      for (int index = 0; index < this.clusterSize; index++) {
        this.clusterEntitiesList.put(index, new ArrayList<>());
      }
      
      // 各エンティティを最も近いセンターに割り当て
      for (StandardEntity entity : entityList) {
        int nearestCenterIndex = this.getNearestCenterIndexFast(entity);
        this.clusterEntitiesList.get(nearestCenterIndex).add(entity);
      }
      
      // センターを更新
      boolean centersChanged = false;
      for (int index = 0; index < this.clusterSize; index++) {
        List<StandardEntity> clusterEntities = this.clusterEntitiesList.get(index);
        if (clusterEntities.isEmpty()) continue;
        
        // 重心計算
        double sumX = 0, sumY = 0;
        for (StandardEntity entity : clusterEntities) {
          Integer entityIndex = this.entityIndexMap.get(entity.getID());
          if (entityIndex != null) {
            sumX += this.entityX[entityIndex];
            sumY += this.entityY[entityIndex];
          }
        }
        double centerX = sumX / clusterEntities.size();
        double centerY = sumY / clusterEntities.size();
        
        // 重心に最も近いエンティティを新しいセンターとする
        StandardEntity newCenter = this.getNearestEntityToPoint(clusterEntities, centerX, centerY);
        
        if (!newCenter.equals(this.centerList.get(index))) {
          this.centerList.set(index, newCenter);
          centersChanged = true;
        }
      }
      
      if (centersChanged) {
        this.updateCenterToIndexMap();
      }
      
      // 早期収束判定
      if (!centersChanged) {
        System.out.println(" Converged at iteration " + (iteration + 1));
        break;
      }
      
      if (scenarioInfo.isDebugMode()) {
        System.out.print("*");
      }
    }

    if (scenarioInfo.isDebugMode()) {
      System.out.println();
    }

    // 最終割り当て
    this.clusterEntitiesList.clear();
    for (int index = 0; index < this.clusterSize; index++) {
      this.clusterEntitiesList.put(index, new ArrayList<>());
    }
    for (StandardEntity entity : entityList) {
      int nearestCenterIndex = this.getNearestCenterIndexFast(entity);
      this.clusterEntitiesList.get(nearestCenterIndex).add(entity);
    }

    // エージェント割り当て
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

    // 結果の構築
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

  /**
   * 高速距離計算（平方根なし）でエンティティに最も近いセンターのインデックスを取得
   */
  private int getNearestCenterIndexFast(StandardEntity entity) {
    Integer entityIndex = this.entityIndexMap.get(entity.getID());
    if (entityIndex == null) {
      // フォールバック
      return 0;
    }
    
    double entityX = this.entityX[entityIndex];
    double entityY = this.entityY[entityIndex];
    
    int nearestIndex = 0;
    double minDistanceSquared = Double.MAX_VALUE;
    
    for (int i = 0; i < this.centerList.size(); i++) {
      StandardEntity center = this.centerList.get(i);
      Integer centerIndex = this.entityIndexMap.get(center.getID());
      if (centerIndex != null) {
        double centerX = this.entityX[centerIndex];
        double centerY = this.entityY[centerIndex];
        
        // 平方根計算を省略（dx*dx + dy*dy）
        double dx = entityX - centerX;
        double dy = entityY - centerY;
        double distanceSquared = dx * dx + dy * dy;
        
        if (distanceSquared < minDistanceSquared) {
          minDistanceSquared = distanceSquared;
          nearestIndex = i;
        }
      }
    }
    
    return nearestIndex;
  }

  /**
   * 指定された座標に最も近いエンティティを取得
   */
  private StandardEntity getNearestEntityToPoint(List<StandardEntity> entities, double targetX, double targetY) {
    StandardEntity nearest = null;
    double minDistanceSquared = Double.MAX_VALUE;
    
    for (StandardEntity entity : entities) {
      Integer entityIndex = this.entityIndexMap.get(entity.getID());
      if (entityIndex != null) {
        double entityX = this.entityX[entityIndex];
        double entityY = this.entityY[entityIndex];
        
        // 平方根計算を省略
        double dx = entityX - targetX;
        double dy = entityY - targetY;
        double distanceSquared = dx * dx + dy * dy;
        
        if (distanceSquared < minDistanceSquared) {
          minDistanceSquared = distanceSquared;
          nearest = entity;
        }
      }
    }
    
    return nearest != null ? nearest : entities.get(0);
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

  private StandardEntity getNearAgent(WorldInfo worldInfo, List<StandardEntity> srcAgentList, StandardEntity targetEntity) {
    StandardEntity result = null;
    double minDistanceSquared = Double.MAX_VALUE;
    
    Integer targetIndex = this.entityIndexMap.get(targetEntity.getID());
    if (targetIndex == null) {
      return srcAgentList.get(0);
    }
    
    double targetX = this.entityX[targetIndex];
    double targetY = this.entityY[targetIndex];
    
    for (StandardEntity agent : srcAgentList) {
      Human human = (Human) agent;
      StandardEntity position = worldInfo.getPosition(human);
      Integer posIndex = this.entityIndexMap.get(position.getID());
      
      if (posIndex != null) {
        double posX = this.entityX[posIndex];
        double posY = this.entityY[posIndex];
        
        double dx = targetX - posX;
        double dy = targetY - posY;
        double distanceSquared = dx * dx + dy * dy;
        
        if (distanceSquared < minDistanceSquared) {
          minDistanceSquared = distanceSquared;
          result = agent;
        }
      }
    }
    
    return result != null ? result : srcAgentList.get(0);
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
