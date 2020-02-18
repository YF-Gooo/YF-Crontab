package master

import (
	"context"
	"time"
	"yf_crontab/crontab/common"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)
	// 构建mongo连接可选属性配置
	opt := new(options.ClientOptions)
	du := time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond
	opt = opt.SetConnectTimeout(du)

	// 建立mongodb连接
	if client, err = mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(G_config.MongodbUri),
		opt); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)

	// len(logArr)
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}
	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	findOptions := options.Find().SetSort(logSort).SetLimit(int64(limit)).SetSkip(int64(skip))
	// 查询
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findOptions); err != nil {
		return
	}
	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}
	return
}
