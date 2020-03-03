package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.javamail.MimeMessageHelper;

import javax.mail.internet.MimeMessage;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * job monitor instance
 *
 * 任务失败处理系统
 * @author xuxueli 2015-9-1 18:05:56
 */
public class JobFailMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobFailMonitorHelper.class);

	// 饿汉式单例模式
	private static JobFailMonitorHelper instance = new JobFailMonitorHelper();
	public static JobFailMonitorHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------

	// 处理失败任务线程
	private Thread monitorThread;
	private volatile boolean toStop = false;
	public void start(){
		monitorThread = new Thread(new Runnable() {

			@Override
			public void run() {

				// monitor
				while (!toStop) {
					try {
						// 查询1000条失败任务日志记录failLogIds：
						// 查询条件：非调度成功或执行成功均需处理
						List<Long> failLogIds = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findFailJobLogIds(1000);
						if (failLogIds!=null && !failLogIds.isEmpty()) {
							// 有则遍历
							for (long failLogId: failLogIds) {

								// lock log
								// CAS乐观锁住单条日志
								int lockRet = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, 0, -1);
								if (lockRet < 1) {
									continue;
								}
								// 加载任务日志全信息
								XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(failLogId);
								// 根据任务日志信息加载任务全信息
								XxlJobInfo info = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(log.getJobId());

								// 需要重试，则进行调度，并更新任务日志中的调度信息
								// 1、fail retry monitor
								if (log.getExecutorFailRetryCount() > 0) {
									JobTriggerPoolHelper.trigger(log.getJobId(), TriggerTypeEnum.RETRY, (log.getExecutorFailRetryCount()-1), log.getExecutorShardingParam(), log.getExecutorParam());
									String retryMsg = "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_type_retry") +"<<<<<<<<<<< </span><br>";
									log.setTriggerMsg(log.getTriggerMsg() + retryMsg);
									XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(log);
								}

								// 有告警邮箱则进行告警------>调用私有failAlarm方法，无邮箱则告警状态为1（无需告警）
								// 告警成功则更新告警状态为2（告警成功），失败则更新为3（告警失败）
								// 2、fail alarm monitor
								int newAlarmStatus = 0;		// 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
								if (info!=null && info.getAlarmEmail()!=null && info.getAlarmEmail().trim().length()>0) {
									boolean alarmResult = true;
									try {
										// 告警方法
										alarmResult = failAlarm(info, log);
									} catch (Exception e) {
										alarmResult = false;
										logger.error(e.getMessage(), e);
									}
									newAlarmStatus = alarmResult?2:3;
								} else {
									newAlarmStatus = 1;
								}
								// CAS更新告警状态
								XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, -1, newAlarmStatus);
							}
						}

					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
						}
					}

					// 阻塞10秒
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                }

				logger.info(">>>>>>>>>>> xxl-job, job fail monitor thread stop");

			}
		});
		monitorThread.setDaemon(true);
		monitorThread.setName("xxl-job, admin JobFailMonitorHelper");
		monitorThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		monitorThread.interrupt();
		try {
			monitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- alarm ----------------------
	// 邮件内容主体模板
	// email alarm template
	private static final String mailBodyTemplate = "<h5>" + I18nUtil.getString("jobconf_monitor_detail") + "：</span>" +
			"<table border=\"1\" cellpadding=\"3\" style=\"border-collapse:collapse; width:80%;\" >\n" +
			"   <thead style=\"font-weight: bold;color: #ffffff;background-color: #ff8c00;\" >" +
			"      <tr>\n" +
			"         <td width=\"20%\" >"+ I18nUtil.getString("jobinfo_field_jobgroup") +"</td>\n" +
			"         <td width=\"10%\" >"+ I18nUtil.getString("jobinfo_field_id") +"</td>\n" +
			"         <td width=\"20%\" >"+ I18nUtil.getString("jobinfo_field_jobdesc") +"</td>\n" +
			"         <td width=\"10%\" >"+ I18nUtil.getString("jobconf_monitor_alarm_title") +"</td>\n" +
			"         <td width=\"40%\" >"+ I18nUtil.getString("jobconf_monitor_alarm_content") +"</td>\n" +
			"      </tr>\n" +
			"   </thead>\n" +
			"   <tbody>\n" +
			"      <tr>\n" +
			"         <td>{0}</td>\n" +
			"         <td>{1}</td>\n" +
			"         <td>{2}</td>\n" +
			"         <td>"+ I18nUtil.getString("jobconf_monitor_alarm_type") +"</td>\n" +
			"         <td>{3}</td>\n" +
			"      </tr>\n" +
			"   </tbody>\n" +
			"</table>";

	/**
	 * fail alarm
	 *
	 * @param jobLog
	 */
	private boolean failAlarm(XxlJobInfo info, XxlJobLog jobLog){
		boolean alarmResult = true;

		// send monitor email
		if (info!=null && info.getAlarmEmail()!=null && info.getAlarmEmail().trim().length()>0) {

			// alarmContent
			String alarmContent = "Alarm Job LogId=" + jobLog.getId();
			if (jobLog.getTriggerCode() != ReturnT.SUCCESS_CODE) {
				// 调度失败
				alarmContent += "<br>TriggerMsg=<br>" + jobLog.getTriggerMsg();
			}
			if (jobLog.getHandleCode()>0 && jobLog.getHandleCode() != ReturnT.SUCCESS_CODE) {
				// 执行失败
				alarmContent += "<br>HandleCode=" + jobLog.getHandleMsg();
			}

			// email info
			// 查询group表为了获取title字段（执行器名称），执行器AppName可读性不强。
			XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(Integer.valueOf(info.getJobGroup()));
			String personal = I18nUtil.getString("admin_name_full");
			String title = I18nUtil.getString("jobconf_monitor");
			// 填充模板
			String content = MessageFormat.format(mailBodyTemplate,
					group!=null?group.getTitle():"null",
					info.getId(),
					info.getJobDesc(),
					alarmContent);
			// 告警邮箱去重，告警邮箱多个时使用英文逗号分隔。
			Set<String> emailSet = new HashSet<String>(Arrays.asList(info.getAlarmEmail().split(",")));
			for (String email: emailSet) {

				// 发送邮件
				// make mail
				try {
					MimeMessage mimeMessage = XxlJobAdminConfig.getAdminConfig().getMailSender().createMimeMessage();

					MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
					// 发件人：调度中心properties文件中配置的邮箱
					helper.setFrom(XxlJobAdminConfig.getAdminConfig().getEmailUserName(), personal);
					// 收件人：Web界面添加任务时填写的邮箱
					helper.setTo(email);
					helper.setSubject(title);
					helper.setText(content, true);

					XxlJobAdminConfig.getAdminConfig().getMailSender().send(mimeMessage);
				} catch (Exception e) {
					logger.error(">>>>>>>>>>> xxl-job, job fail alarm email send error, JobLogId:{}", jobLog.getId(), e);

					alarmResult = false;
				}

			}
		}

		// do something, custom alarm strategy, such as sms
		// 扩展 自定义告警方式

		return alarmResult;
	}

}
