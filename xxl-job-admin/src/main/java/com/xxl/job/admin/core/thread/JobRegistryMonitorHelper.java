package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * job registry instance
 * 注册中心心跳续约服务
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobRegistryMonitorHelper.class);

	// “饿汉式”单例模式
	private static JobRegistryMonitorHelper instance = new JobRegistryMonitorHelper();
	public static JobRegistryMonitorHelper getInstance(){
		return instance;
	}

	private Thread registryThread;
	private volatile boolean toStop = false;
	public void start(){
		registryThread = new Thread(new Runnable() {
			@Override
			public void run() {
				// 死循环
				while (!toStop) {
					try {
						// auto registry group 查询地址类型为自动注册的执行器信息列表。xxl_job_group表
						List<XxlJobGroup> groupList = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);

						// “心跳”机制，xxl_job_registry为“心跳”注册表
						if (groupList!=null && !groupList.isEmpty()) {
							// remove dead address (admin/executor)
							// 查询“心跳”注册表中上次更新时间（“心跳”时间）超过90秒的注册ids集合
							// 传入Java时间以屏蔽与数据库系统之间可能存在的时间差
							List<Integer> ids = XxlJobAdminConfig.getAdminConfig()
									.getXxlJobRegistryDao()
									.findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
							if (ids!=null && ids.size()>0) {
								// 删除“心跳死亡”的注册表
								XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
							}

							// fresh online address (admin/executor) 刷新在线机器地址
							HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
							// 查询上次更新时间（上次“心跳”时间）在90秒以内的注册列表
							// 查询“存活的”：“心跳”续约
							List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig()
									.getXxlJobRegistryDao()
									.findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
							if (list != null) {
								// 遍历
								for (XxlJobRegistry item: list) {
									// 如果注册组类型是执行器类型
									if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
										// 注册表的注册key（registry_key字段）为执行器名称
										String appName = item.getRegistryKey();
										// 先尝试从局部map变量中取出该执行器对应注册地址集合
										List<String> registryList = appAddressMap.get(appName);
										if (registryList == null) {
											// 没有则新建一个空集合，懒加载。
											registryList = new ArrayList<String>();
										}
										// 如果从map中取出来的注册地址集合中不包含当前注册地址则添加至注册地址集合
										if (!registryList.contains(item.getRegistryValue())) {
											// 去重
											registryList.add(item.getRegistryValue());
										}
										// 将执行器名称和注册地址集合映射至局部map变量
										appAddressMap.put(appName, registryList);
									}
								}
							}

							// fresh group address 遍历自动注册的执行器信息列表
							for (XxlJobGroup group: groupList) {
								// 从局部map变量中取出对应执行器名称的注册地址集合
								List<String> registryList = appAddressMap.get(group.getAppName());
								String addressListStr = null;
								// 组装执行器地址列表，多地址逗号分隔
								if (registryList!=null && !registryList.isEmpty()) {
									// 排序？
									Collections.sort(registryList);
									addressListStr = "";
									// 遍历注册地址List集合组装成字符串并以逗号分隔
									for (String item:registryList) {
										// 更优雅规范写法？StringBuilder？
										addressListStr += item + ",";
									}
									// 截取字符串最后一个逗号
									addressListStr = addressListStr.substring(0, addressListStr.length()-1);
								}
								// 将注册地址列表信息更新至DB xxl_job_group表
								group.setAddressList(addressListStr);
								XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
							}
						}
					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
					try {
						// 阻塞：睡眠30秒后进入下一次循环
						TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
					} catch (InterruptedException e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
						}
					}
				}
				logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
			}
		});
		// 守护线程
		registryThread.setDaemon(true);
		// 设置线程名
		registryThread.setName("xxl-job, admin JobRegistryMonitorHelper");
		// 启动
		registryThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		// 给registryThread线程设置中断标记，并没有真的停止。
		registryThread.interrupt();
		try {
			registryThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

}
