#!/bin/sh

################ Version Info ##################
# Create Date: 2018-06-14
# Author:      ZhangLiang
# Mail:        zhanglianghhh@163.com
# Version:     1.0
# Attention:   上传文件到HDFS
################################################
# V1.0.1 2017-06-26 脚本编写    zhangliang
#      1、上传文件到HDFS
################################################
# 1、如有需要可以在HDFS目录中添加小时
# 2、如果必要可以添加邮件、微信、短信告警
################################################

# 环境变量
. /etc/profile
. ~/.bash_profile

# 日志文件存放的目录
log_src_dir="/app/webservice/logs"

# 待上传文件存放的目录
log_toupload_dir="/app/webservice/logs/up2hdfs"

# 日志文件上传到hdfs的根路径
hdfs_root_dir="/data/webservice/$(date +%Y%m%d)/"

# 目录信息
script_dir=$( cd "$( dirname "$0"  )" && pwd )
script_name=$(basename ${0})
# 日志目录
[ ! -d ${script_dir}/log/ ] && {
  mkdir -p ${script_dir}/log/
}
script_log="${script_dir}/log/${script_name}.log"

# 时间信息
datetimenow="$(date +%Y%m%d%H%M%S)"

# 写日志
writelog(){
  loginfo="$1"
  datetime_now2="$(date '+%F %T')"      # 如：2017-03-28 14:15:46

  echo "${datetime_now2} ${loginfo}"
  echo "${datetime_now2} ${script_name} ${loginfo}" >> ${script_log}
}

# 执行函数
exe_fun(){
  # 判断 HDFS 响应的目录是否存在
  hdfs_flag=$(hadoop fs -ls ${hdfs_root_dir} | grep "${hdfs_root_dir}" | wc -l)
  if [ ${hdfs_flag} -eq 0 ]; then
    # 没有对应的目录
    hadoop fs -mkdir -p ${hdfs_root_dir}
  fi

  # 对要上传的日志进行转移， 转移到 ${log_toupload_dir} 
  up_accesslog_arr=$(cd ${log_src_dir} && ls access.log.*)
  for up_accesslog in ${up_accesslog_arr[*]};do
    # 转移日志  重命名，后期上传到HDFS
    mv ${log_src_dir}/${up_accesslog} ${log_toupload_dir}/${up_accesslog}_${datetimenow}

  done

  # 进入 待上传文件存放的目录  判断日志是否上传，如果没有上传那么上传
  cd ${log_toupload_dir} && {
    # 过滤出未上传的日志
    up_df_log_arr=$(ls access.log.* | grep -v 'DONE')

    for up_df_log in ${up_df_log_arr[*]}; do
      hadoop fs -put ${up_df_log} ${hdfs_root_dir} 

      ok_flag=$?
      # 如果上传失败， 记录
      if [ ${ok_flag} != "0" ]; then
        echo "${up_df_log}  fail" >> ${log_toupload_dir}/upFail_${datetimenow}
        writelog "${up_df_log}        fail"  # 脚本日志记录
      else
        # 重命名
        mv ${up_df_log} ${up_df_log}_DONE

        writelog "${up_df_log}        ok"    # 脚本日志记录
      fi
   done
  }

}


main(){
  exe_fun
}

main

