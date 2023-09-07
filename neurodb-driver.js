const NEURODB_RETURNDATA = 1;
const NEURODB_SELECTDB = 2;
const NEURODB_EOF = 3;
const NEURODB_NODES = 6;
const NEURODB_LINKS = 7;
const NEURODB_EXIST = 17;
const NEURODB_NIL = 18;
const NEURODB_RECORD = 19;
const NEURODB_RECORDS = 20;

const NDB_6BITLEN = 0;
const NDB_14BITLEN = 1;
const NDB_32BITLEN = 2;
const NDB_ENCVAL = 3;
//const NDB_LENERR =UINT_MAX;

const VO_STRING = 1;
const VO_NUM = 2;
const VO_STRING_ARRY = 3;
const VO_NUM_ARRY = 4;
const VO_NODE = 5;
const VO_LINK = 6;
const VO_PATH = 7;
const VO_VAR = 8;
const VO_VAR_PATTERN = 9;

/*定义表1 返回结果类型*/
const STATUS_DIC = [
   { status: "OK", msg: "运行成功" }, // 1 
   { status: "NO_MEM_ERR", msg: "内存分配异常" },// 2
   { status: "SYNTAX_ERR", msg: "普通语法错误" },// 3
   { status: "NO_Exp_ERR", msg: "未找到此指令" },// 4  
   { status: "NO_LNK_ERR", msg: "缺少关系" },// 5 
   { status: "NO_ARROW_ERR", msg: "缺少箭头" },// 6   
   { status: "DOU_ARROW_ERR", msg: "关系双箭头错误" },//7   
   { status: "NO_HEAD_ERR", msg: "缺少头节点" },// 8    
   { status: "NO_TAIL_ERR", msg: "缺少尾结点" },// 9     
   { status: "CHAR_NUM_UL_ERR", msg: "必须是字母数字下划线" },// 10     
   { status: "NOT_PATTERN_ERR", msg: "不是模式表达式" },// 11  
   { status: "DUP_VAR_NM_ERR", msg: "此变量已被使用" },// 12  
   { status: "NO_SM_TYPE_ERR", msg: "数组中含有不相同的类型" },// 13    
   { status: "NO_SUP_TYPE", msg: "不支持的数据类型" },// 14
   { status: "WRON_EXP", msg: "指令搭配错误" },// 15
   { status: "NOT_SUPPORT", msg: "暂不支持的指令" },// 16      
   { status: "WHERE_SYN_ERR", msg: "where语句语法" },// 17  
   { status: "WHERE_RUN_ERR", msg: "where运算语法" },// 18
   { status: "NO_VAR_ERR", msg: "未找到变量" },// 19 
   { status: "NO_PAIR_BRK", msg: "缺失配对括号" },// 20   
   { status: "CLIST_HAS_LINK_ERR", msg: "删除带有关边的节点" },// 21
   { status: "CLIST_OPR_ERR", msg: "数据操作错误" },// 22
   { status: "ORDER_BY_SYN_ERR", msg: "order by语句语法" },// 23 
   { status: "DEL_PATH_ERR", msg: "不可删除路径" },// 24
   { status: "UNDEFINED_VAR_ERR", msg: "未定义的变量" },// 25 
   { status: "WHERE_PTN_NO_VAR_ERR", msg: "where 模式条件，独立连通图缺少变量" },// 26
   { status: "NO_PROC_ERR", msg: "不支持的存储过程" },// 27  
   { status: "CSV_FILE_ERR", msg: "csv文件读取错误" },// 28
   { status: "CSV_ROW_VAR_ERR", msg: "csv 变量属性名在列中未找到" },// 29 
   { status: "QUREY_TIMEOUT_ERR", msg: "查询过载超时" },// 30 
]

let NeuroDBDriver = function (url,onOpen,onMessage,onError,onClose) {
   let that = this;
   this.query = null;
   this.url=url;
   this.onOpen=onOpen;
   this.onMessage=onMessage;
   this.onError=onError;
   this.onClose=onClose;
   this.isInit=true;
   this.webSocketInit();   
};

NeuroDBDriver.prototype.webSocketInit=function(){
   let that=this
   let ws = new WebSocket('ws://' + this.url);
   ws.onopen = function (event) {
      console.log("connection is ready")
      if (that.onOpen){that.onOpen();}
      if (that.query) { ws.send(that.query); }
   };
   ws.onmessage = function (event) {
      let d = deserializeReturnData(event.data)
      console.log(d);
      that.onMessage(d);
   };
   ws.onerror = function (error) {
      console.warn("connection error");
      if(that.onError){that.onError(error);} 
   };
   ws.onclose = function (event) {
      console.log("connection is closed");
      if(that.onClose){that.onClose(event);}
   };
   this.ws = ws;
}

NeuroDBDriver.prototype.executeQuery = function (query) {
   console.log(query);
   let that = this;
   if (this.ws.readyState == 1) {
      this.ws.send(query);
   } else {
      this.query = query;
      if(!this.isInit){
         this.webSocketInit();
      }
   }
   this.isInit=false;
   return new Promise(function (resolve, reject) {
      that.onMessage = function (data) {
         resolve(data);
      }
      that.onerror = function (error) {
         reject(error);
      }
   });
};

NeuroDBDriver.prototype.close = function () {
   this.ws.close();
}

let StringCur = function (s) {
   this.s = s;
   this.cur = 0;
   this.byteIndex = 0;
}
StringCur.prototype.get = function (size) {
   if(size==0) return "";
   let start = this.cur
   let end = this.getUTF8StrIndex(size,true);
   let subStr = this.s.substring(start, end);
   return subStr;
}
StringCur.prototype.getType = function () {
   let index =  this.getUTF8StrIndex(1,false);
   let type = this.s.charCodeAt(index);
   return type;
}

StringCur.prototype.getUTF8StrIndex=function( byteSize,skip) {

   let charCode,tempByteIndex=this.byteIndex, tempCur=this.cur;
   while (this.cur < this.s.length) {
      charCode = this.s.codePointAt(this.cur);
      if (charCode <= 0x007f) {
         tempByteIndex += 1;//字符代码在000000 – 00007F之间的，用一个字节编码
      } else if (charCode <= 0x07ff) {
         tempByteIndex+= 2;//000080 – 0007FF之间的字符用两个字节
      } else if (charCode <= 0xffff) {
         tempByteIndex += 3;//000800 – 00D7FF 和 00E000 – 00FFFF之间的用三个字节，注: Unicode在范围 D800-DFFF 中不存在任何字符
      } else {
         tempByteIndex += 4;//010000 – 10FFFF之间的用4个字节
         this.cur++;
      }
      this.cur++;      
      if (tempByteIndex >= this.byteIndex+byteSize) break;
   }
   this.byteIndex=tempByteIndex;
   if(skip)
      return this.cur
   else
      return tempCur;
}


function deserializeType(cur) {
   return cur.getType();
}

function deserializeUint(cur)
{
   let buf = [0,0,0];
   buf[0] = cur.get(1).charCodeAt(0);;
   buf[1] = cur.get(1).charCodeAt(0);;
   buf[2] = cur.get(1).charCodeAt(0);;
   return (buf[0]&0x7f)<<14|(buf[1]&0x7f)<<7|buf[2];
}


function deserializeString(cur) {
   let len = deserializeUint(cur);
   return cur.get(len);
}

function deserializeStringList(cur) {
   let listlen = deserializeUint(cur);
   let l = [];
   while (listlen-- > 0) {
      let s = deserializeString(cur);
      l.push(s);
   }
   return l;
}

function deserializeLabels(cur, labeList) {
   let listlen = deserializeUint(cur);
   let l = [];
   while (listlen-- > 0) {
      let i = deserializeUint(cur);
      l.push(labeList[i]);
   }
   return l;
}

function deserializeKVList(cur, keyNames) {
   let listlen = deserializeUint(cur);
   let properties = {};
   while (listlen-- > 0) {
      let index = deserializeUint(cur);
      let key = keyNames[index];
      let type = deserializeUint(cur);
      let aryLen = 0;
      let val = null;
      if (type == VO_STRING) {
         val = deserializeString(cur);
      } else if (type == VO_NUM) {
         let num = deserializeString(cur);
         val = parseFloat(num);
      } else if (type == VO_STRING_ARRY) {
         aryLen = deserializeUint(cur);
         let valAry = [];
         for (let k = 0; k < aryLen; k++) {
            let str = deserializeString(cur)
            valAry.push(str);
         }
         val = valAry;
      } else if (type == VO_NUM_ARRY) {
         aryLen = deserializeUint(cur);
         let valAry = new double[aryLen];
         for (let k = 0; k < aryLen; k++) {
            let doubleStr = deserializeString(cur);
            let doubleVal = parseFloat(doubleStr);
            valAry.push(doubleVal);
         }
         val = valAry;
      } else {
         throw "Error Type";
      }
      properties[key] = val;
   }
   return properties;
}

function deserializeCNode(cur, labels, keyNames) {
   let id = deserializeUint(cur);
   let nlabels = deserializeLabels(cur, labels);
   let properties = deserializeKVList(cur, keyNames);
   let n = { id: id, labels: nlabels, properties: properties };
   return n;
}

function deserializeCLink(cur, types, keyNames) {
   let id = deserializeUint(cur);
   let hid = deserializeUint(cur);
   let tid = deserializeUint(cur);
   let ty = deserializeType(cur);
   let type = null;
   if (ty == NEURODB_EXIST) {
      let typeIndex = deserializeUint(cur);
      type = types[typeIndex];
   } else if (ty == NEURODB_NIL) {
   }
   let properties = deserializeKVList(cur, keyNames);
   let l = { id: id, hid: hid, tid: tid, type: type, properties: properties };
   return l;
}

function deserializeBodyData(body) {
   let cur = new StringCur(body);
   let rd = { nodes: [], links: [], records: [] };
   /*读取labels、types、keyNames列表*/
   if (deserializeType(cur) != NEURODB_RETURNDATA)
      throw "Error Type";
   rd.labels = deserializeStringList(cur);
   rd.types = deserializeStringList(cur);
   rd.keyNames = deserializeStringList(cur);
   /*读取节点列表*/
   if (deserializeType(cur) != NEURODB_NODES)
      throw "Error Type";
   let cnt_nodes = deserializeUint(cur);
   for (let i = 0; i < cnt_nodes; i++) {
      let n = deserializeCNode(cur, rd.labels, rd.keyNames);
      rd.nodes.push(n);
   }
   /*读取关系列表*/
   if (deserializeType(cur) != NEURODB_LINKS)
      throw "Error Type";
   let cnt_links = deserializeUint(cur);
   for (let i = 0; i < cnt_links; i++) {
      let l = deserializeCLink(cur, rd.types, rd.keyNames);
      rd.links.push(l);
   }
   /*读取return结果集列表*/
   if (deserializeType(cur) != NEURODB_RECORDS)
      throw "Error Type";
   let cnt_records = deserializeUint(cur);
   for (let i = 0; i < cnt_records; i++) {
      let type, cnt_column;
      if (deserializeType(cur) != NEURODB_RECORD)
         throw "Error Type";
      cnt_column = deserializeUint(cur);
      let record = [];
      for (let j = 0; j < cnt_column; j++) {
         let aryLen = 0;
         type = deserializeType(cur);
         let val = null;
         if (type == NEURODB_NIL) {
            /*val =NULL;*/
         } else if (type == VO_NODE) {
            let id = deserializeUint(cur);
            let n = rd.nodes.find(item => item.id == id);
            val = n;
         } else if (type == VO_LINK) {
            let id = deserializeUint(cur);
            let l = rd.links.find(item => item.id == id);
            val = l;
         } else if (type == VO_PATH) {
            let len = deserializeUint(cur);
            let path = [];
            for (let k = 0; k < len; k++) {
               let id = deserializeUint(cur);
               if (k % 2 == 0) {
                  let nd = rd.nodes.find(item => item.id == id);
                  path.push(nd);
               } else {
                  let lk = rd.links.find(item => item.id == id);
                  path.push(lk);
               }
            }
            val = path;
         } else if (type == VO_STRING) {
            val = deserializeString(cur);
         } else if (type == VO_NUM) {
            let num = deserializeString(cur);
            val = parseFloat(num);
         } else if (type == VO_STRING_ARRY) {
            aryLen = deserializeUint(cur);
            let valAry = [];
            for (let k = 0; k < aryLen; k++) {
               let str = deserializeString(cur)
               valAry.push(str);
            }
            val = valAry;
         } else if (type == VO_NUM_ARRY) {
            aryLen = deserializeUint(cur);
            let valAry = new double[aryLen];
            for (let k = 0; k < aryLen; k++) {
               let doubleStr = deserializeString(cur);
               let doubleVal = parseFloat(doubleStr);
               valAry.push(doubleVal);
            }
            val = valAry;
         } else {
            throw "Error Type";
         }
         record.push(val);
      }
      rd.records.push(record);
   }
   /*读取结束标志*/
   if (deserializeType(cur) != NEURODB_EOF)
      throw "Error Type";
   return rd;
}

function deserializeReturnData(str) {
   let resultSet = {};
   let type = str.charAt(0);
   let brIndex = -1;
   switch (type) {
      case '@': /* 返回的是只有一个成功执行状态位的数据包*/
         resultSet.status = 1;
         resultSet.status = 'OK';
         resultSet.msg='运行成功'
         break;
      case '$': /* 返回的是包含错误消息的报错数据包*/
         resultSet.status = 0;
         resultSet.status = 'ERR';
         brIndex = str.indexOf("\r\n");
         resultSet.msg = str.substring(brIndex);
         break;
      case '#': /* 返回的是包含正常消息的消息数据包*/
         resultSet.status = 1;
         brIndex = str.indexOf("\r\n");
         resultSet.msg = str.substring(brIndex);
         break;
      case '*': /* 返回的是图查询结果数据包 */
         brIndex = str.indexOf("\n");
         let line = str.substring(1, brIndex);
         let head = line.split(",");
         let statusCode = parseInt(head[0]);
         resultSet.cursor = parseInt(head[1]);

         let obj= STATUS_DIC[statusCode- 1];
         resultSet.status =obj.status;
         resultSet.msg=obj.msg;
         if(statusCode!=1){
            break;
         }
         resultSet.results = parseInt(head[2]);
         resultSet.addNodes = parseInt(head[3]);
         resultSet.addLinks = parseInt(head[4]);
         resultSet.modifyNodes = parseInt(head[5]);
         resultSet.modifyLinks = parseInt(head[6]);
         resultSet.deleteNodes = parseInt(head[7]);
         resultSet.deleteLinks = parseInt(head[8]);
         //resultSet.recordSet ={};

         if (resultSet.results > 0) {
            let bodyStr = str.substring(brIndex + 1)
            let recordSet = deserializeBodyData(bodyStr);
            resultSet.recordSet = recordSet;
         }
         break;
      default:
         throw new Exception("reply type error");
   }

   return resultSet;
}



export default NeuroDBDriver