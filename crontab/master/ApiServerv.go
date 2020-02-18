package master

import (
	"encoding/json"
	"net/http"
	"strconv"
	"yf_crontab/crontab/common"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *gin.Engine
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job={"name": "job1", "command": "echo hello", "cronExpr": "* * * * *"}
func handleJobSave(c *gin.Context) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		res     common.Response
	)

	// 2, 取表单中的job字段
	postJob = c.PostForm("job")
	// 3, 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	// 4, 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}
	// 5, 返回正常应答 ({"errno": 0, "msg": "", "data": {....}})
	if res = common.BuildResponse(0, "success", oldJob); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return
ERR:
	// 6, 返回异常应答
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// 删除任务接口
// POST /job/delete   name=job1
func handleJobDelete(c *gin.Context) {
	var (
		err    error // interface{}
		name   string
		oldJob *common.Job
		res    common.Response
	)

	// 删除的任务名
	name = c.PostForm("name")

	// 去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	// 正常应答
	if res = common.BuildResponse(0, "success", oldJob); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return

ERR:
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// 列举所有crontab任务
func handleJobList(c *gin.Context) {
	var (
		jobList []*common.Job
		res     common.Response
		err     error
	)

	// 获取任务列表
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}

	// 正常应答
	if res = common.BuildResponse(0, "success", jobList); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return

ERR:
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// 强制杀死某个任务
// POST /job/kill  name=job1
func handleJobKill(c *gin.Context) {
	var (
		err  error
		name string
		res  common.Response
	)

	// 要杀死的任务名
	name = c.PostForm("name")

	// 杀死任务
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	// 正常应答
	if res = common.BuildResponse(0, "success", nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return

ERR:
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// 查询任务日志
func handleJobLog(c *gin.Context) {
	var (
		err        error
		name       string // 任务名字
		skipParam  string // 从第几条开始
		limitParam string // 返回多少条
		skip       int
		limit      int
		logArr     []*common.JobLog
		res        common.Response
	)

	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name = c.Query("name")
	skipParam = c.Query("skip")
	limitParam = c.Query("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}
	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}
	// 正常应答
	if res = common.BuildResponse(0, "success", logArr); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return

ERR:
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// 获取健康worker节点列表
func handleWorkerList(c *gin.Context) {
	var (
		workerArr []string
		err       error
		res       common.Response
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	// 正常应答
	if res = common.BuildResponse(0, "success", workerArr); err == nil {
		c.JSON(http.StatusOK, res)
	}
	return

ERR:
	if res = common.BuildResponse(-1, err.Error(), nil); err == nil {
		c.JSON(http.StatusOK, res)
	}
}

// Cors 跨域配置
func Cors() gin.HandlerFunc {
	config := cors.DefaultConfig()
	config.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Cookie"}
	config.AllowAllOrigins = true
	config.AllowCredentials = true
	return cors.New(config)
}

// 初始化服务
func InitApiServer() (err error) {
	r := gin.Default()

	// 配置路由
	r.Use(Cors())
	r.Static("/static", "./webroot/")
	r.POST("/job/save", handleJobSave)
	r.POST("/job/delete", handleJobDelete)
	r.GET("/job/list", handleJobList)
	r.POST("/job/kill", handleJobKill)
	r.GET("/job/log", handleJobLog)
	r.GET("/worker/list", handleWorkerList)
	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: r,
	}
	go r.Run(":" + strconv.Itoa(G_config.ApiPort))
	return
}
