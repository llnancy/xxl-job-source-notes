package com.xxl.job.admin.core.conf;

import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.test.JobFailMonitorHelper;
import com.xxl.job.admin.dao.*;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;

/**
 * xxl-job config
 *
 * 调度中心配置“容器”，存储调度中心各种依赖对象。
 * 实现了Spring的生命周期回调接口：InitializingBean, DisposableBean
 * @author xuxueli 2017-04-28
 */
@Component
public class XxlJobAdminConfig implements InitializingBean, DisposableBean {

    /**
     * 当前类对象。
     * 为什么要有这个对象？
     * 不使用Ioc的依赖注入，使用XxlJobAdminConfig.getAdminConfig()获取该类对象。
     */
    private static XxlJobAdminConfig adminConfig = null;
    public static XxlJobAdminConfig getAdminConfig() {
        return adminConfig;
    }


    // ---------------------- XxlJobScheduler ----------------------

    /**
     * 任务调度器
     */
    private XxlJobScheduler xxlJobScheduler;

    /**
     * Spring Bean的生命周期初始回调，初始化该bean对象时会调用该方法。
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 赋值，此时this引用是否是完整的对象？Spring提前暴露引用？
        adminConfig = this;
        // 创建调度器并初始化
        xxlJobScheduler = new XxlJobScheduler();
        xxlJobScheduler.init();
        JobFailMonitorHelper.getInstance();
    }

    /**
     * Spring Bean的生命周期销毁回调，销毁该bean对象时会调用该方法。
     * 销毁调度器
     * 优雅停止线程？
     */
    @Override
    public void destroy() throws Exception {
        xxlJobScheduler.destroy();
    }


    // ---------------------- XxlJobScheduler ----------------------

    // conf
    // 调度中心application.properties配置项
    @Value("${xxl.job.i18n}")
    private String i18n;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    @Value("${spring.mail.username}")
    private String emailUserName;

    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;

    @Value("${xxl.job.logretentiondays}")
    private int logretentiondays;

    // dao, service
    // 调度中心所使用到的服务对象
    @Resource
    private XxlJobLogDao xxlJobLogDao;
    @Resource
    private XxlJobInfoDao xxlJobInfoDao;
    @Resource
    private XxlJobRegistryDao xxlJobRegistryDao;
    @Resource
    private XxlJobGroupDao xxlJobGroupDao;
    @Resource
    private XxlJobLogReportDao xxlJobLogReportDao;
    @Resource
    private JavaMailSender mailSender;
    @Resource
    private DataSource dataSource;


    public String getI18n() {
        return i18n;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getEmailUserName() {
        return emailUserName;
    }

    public int getTriggerPoolFastMax() {
        if (triggerPoolFastMax < 200) {
            return 200;
        }
        return triggerPoolFastMax;
    }

    public int getTriggerPoolSlowMax() {
        if (triggerPoolSlowMax < 100) {
            return 100;
        }
        return triggerPoolSlowMax;
    }

    public int getLogretentiondays() {
        if (logretentiondays < 7) {
            return -1;  // Limit greater than or equal to 7, otherwise close
        }
        return logretentiondays;
    }

    public XxlJobLogDao getXxlJobLogDao() {
        return xxlJobLogDao;
    }

    public XxlJobInfoDao getXxlJobInfoDao() {
        return xxlJobInfoDao;
    }

    public XxlJobRegistryDao getXxlJobRegistryDao() {
        return xxlJobRegistryDao;
    }

    public XxlJobGroupDao getXxlJobGroupDao() {
        return xxlJobGroupDao;
    }

    public XxlJobLogReportDao getXxlJobLogReportDao() {
        return xxlJobLogReportDao;
    }

    public JavaMailSender getMailSender() {
        return mailSender;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

}
