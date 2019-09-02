using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Configuration;

namespace MongoHelp
{
    public class MongoHelper
    {

        private readonly MongoDatabase _db;
        private readonly string host = ConfigurationManager.AppSettings["db_host"];
        private readonly string db_name = ConfigurationManager.AppSettings["db_name"];

        private readonly ObjectId id;
        public MongoHelper()
        {
            var client = new MongoClient(host); //ip及端口
            var server = client.GetServer();
            this._db = server.GetDatabase(db_name); //数据库名称
        }

        /// <summary>
        /// 新增单个实体模型 modifiy by yuxl 2018-02-24
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns>True:成功,False:失败</returns>
        public bool Insert<T>(T entity)
        {
            bool isOk = true;
            try
            {
                BsonDocument doc = entity.ToBsonDocument();
                WriteConcernResult result = this._db.GetCollection(typeof(T).Name).Insert(doc);
                isOk = result.Ok;
            }
            catch (Exception ex)
            {

            }
            return isOk;
        }
      

        /// <summary>
        /// 新增实体集合模型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns>True:成功,False:失败</returns>
        public bool Add<T>(IEnumerable<T> entity)
        {
            bool isOk = true;
            try
            {
                int s = 1;
                IEnumerable<WriteConcernResult> results = this._db.GetCollection(typeof(T).Name).InsertBatch(entity);
                foreach (var item in results)
                {
                    if (item.Ok)
                        isOk = true;
                    else
                        isOk = false;
                }
            }
            catch (Exception ex)
            {

            }
            return isOk;
        }

        /// <summary>
        /// 查询单条数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Field">查询属性字段</param>
        /// <param name="Value">字段值</param>
        /// <returns>返回当前实体</returns>
        public T FindOne<T>(string Field, string Value)
        {
            T oneEntity = default(T);
            try
            {

                FindOneArgs args = new FindOneArgs
                {
                    Query = Query.EQ(Field, Value)
                };
                oneEntity = this._db.GetCollection(typeof(T).Name).FindOneAs<T>(args);
                this._db.GetCollection(typeof(T).Name).FindAs<T>(Query.GTE(Field, Value)).ToList();
            }
            catch (Exception ex)
            {

            }
            return oneEntity;
        }

        /// <summary>
        /// 查询多条数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Field">查询属性字段</param>
        /// <param name="Value">字段值</param>
        /// <returns>返回当前实体集合</returns>
        public List<T> FindMore<T>(string Field, string Value)
        {
            List<T> list = new List<T>();
            try
            {
                list = this._db.GetCollection(typeof(T).Name).FindAs<T>(Query.GTE(Field, Value)).ToList();
            }
            catch (Exception ex)
            {

            }
            return list;
        }

        /// <summary>
        /// 查询文档所有数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public List<T> FindAllMore<T>()
        {
            List<T> list = new List<T>();
            try
            {
             
                list = this._db.GetCollection(typeof(T).Name).FindAllAs<T>().ToList();
            }
            catch (Exception ex)
            {

            }
            return list;
        }

        /// <summary>
        /// 分页查询文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="pageIndex">页次</param>
        /// <param name="pageRow">每页显示记录数</param>
        /// <param name="total">总记录数</param>
        /// <returns></returns>
        public List<T> FindMoreForPage<T>(int pageIndex, int pageRow, ref long total)
        {
            List<T> list = new List<T>();
            try
            {
                total = this._db.GetCollection(typeof(T).Name).FindAllAs<T>().Count();    //获取总记录数
                if (pageIndex == 1)
                {
                    list = this._db.GetCollection(typeof(T).Name).FindAllAs<T>().SetLimit(pageRow).ToList();
                }
                else
                {
                    var bd = this._db.GetCollection(typeof(T).Name).FindAll().SetSortOrder("_id:1").SetLimit((pageIndex - 1) * pageRow).Last();   //获取最后一个ID主键
                    var el = bd.GetElement(0);
                    var value = el.Value;
                    list = this._db.GetCollection(typeof(T).Name).FindAs<T>(Query.GT("_id", value)).SetSortOrder("_id:1").SetLimit(pageRow).ToList();

                }
            }
            catch (Exception ex)
            {

            }
            return list;
        }
        /// <summary>
        /// yuxl by 2018 ADD
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">查询条件</param>
        /// <param name="pageIndex">当前页</param>
        /// <param name="rows">行数</param>
        /// <param name="totalCount">总行数</param>
        /// <returns></returns>
        public List<T> FindPageNewAdd<T>(IMongoQuery query,  int pageIndex, int rows, ref long totalCount)
        {
            List<T> list = new List<T>();
            int skipCount = 0;
            try 
	        {	    
                if(pageIndex>1)
                {
                     skipCount = (pageIndex-1)*rows;
                }
                else
                {
                    pageIndex =1;
                }
		        totalCount = this._db.GetCollection(typeof(T).Name).FindAs<T>(query).Count(); 
                if(totalCount>0)
                {
                     list = this._db.GetCollection(typeof(T).Name).FindAs<T>(query).SetFlags(QueryFlags.NoCursorTimeout).SetSortOrder().SetSkip(skipCount).SetLimit(rows).ToList();
                }
	        }
	        catch (Exception)
	        {
		
		        throw;
	        }
            return list;
        }


        /// <summary>
        /// 按条件分页查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">查询条件</param>
        /// <param name="pageIndex">页次</param>
        /// <param name="pageRow">每页显示记录数</param>
        /// <param name="total">总记录数</param>
        /// <returns></returns>
        public List<T> FindMoreForPageByCondi<T>(IMongoQuery query, int pageIndex, int pageRow, ref long total)
        {
            List<T> list = new List<T>();
            try
            {
                total = this._db.GetCollection(typeof(T).Name).FindAs<T>(query).Count();//获取总记录数
                if (pageIndex == 1)
                {
                    list = this._db.GetCollection(typeof(T).Name).FindAs<T>(query).SetSortOrder(SortBy.Descending("CreateDate")).SetLimit(pageRow).ToList();
                }
                else
                {
                    var bd = this._db.GetCollection(typeof(T).Name).Find(query).SetSortOrder("_id:1").SetLimit((pageIndex - 1) * pageRow).Last();   //获取最后一个ID主键
                    var el = bd.GetElement(0);
                    var value = el.Value;
                    list = this._db.GetCollection(typeof(T).Name).FindAs<T>(query).SetSortOrder("_id:1").SetSkip(pageRow).SetLimit(pageRow).ToList();
                }
            }
            catch (Exception ex)
            {

            }
            return list;
        }


        /// <summary>
        /// 更新实体单个字段值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="whereField">条件字段</param>
        /// <param name="whereValue">条件字段值</param>
        /// <param name="updateField">修改字段</param>
        /// <param name="updateValue">修改字段值</param>
        /// <returns></returns>
        public bool UpdateEntity<T>(string whereField, string whereValue, string updateField, string updateValue)
        {
            bool isOk = true;
            try
            {
                var query = Query.EQ(whereField, whereValue);
                var update = Update.Set(updateField, updateValue);
                WriteConcernResult result = this._db.GetCollection(typeof(T).Name).Update(query, update);
                if (!result.Ok)
                    isOk = false;
            }
            catch (Exception ex)
            {

            }
            return isOk;
        }

        /// <summary>
        /// 更新整个实体模型字段
        /// </summary>
        /// <typeparam name="T">泛型参数</typeparam>
        /// <param name="whereField">条件字段</param>
        /// <param name="whereValue">条件值</param>
        /// <param name="updateEntity">实体模型</param>
        /// <returns>True:成功,False:失败</returns>
        public bool UpdateEntityMoreFields<T>(string whereField, string whereValue, T updateEntity)
        {
            bool isOk = true;
            try            {
              
                var query = Query.EQ(whereField, whereValue);
                BsonDocument bsonDoc = updateEntity.ToBsonDocument(typeof(T));
                var update = new UpdateDocument{
                    {"$set",bsonDoc}
                };
                WriteConcernResult result = this._db.GetCollection(typeof(T).Name).Update(query, update);
                if (!result.Ok)
                    isOk = false;
            }
            catch (Exception ex)
            {

            }
            return isOk;
        }

        /// <summary>
        /// 删除实体文档
        /// </summary>
        /// <typeparam name="T">泛型参数</typeparam>
        /// <param name="whereField">条件字段</param>
        /// <param name="whereValue">条件值</param>
        /// <returns></returns>
        public bool DelEntity<T>(string whereField, string whereValue)
        {
            bool isOk = true;
            try
            {
       
                var query = Query.EQ(whereField, whereValue);
                WriteConcernResult result = this._db.GetCollection(typeof(T).Name).Remove(query);
                if (!result.Ok)
                    isOk = false;
            }
            catch (Exception ex)
            {

            }
            return isOk;

        }
    }
}
