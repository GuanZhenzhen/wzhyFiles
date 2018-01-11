package main

import (
	"database/sql"
	"encoding/hex"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"fmt"

	"os"
	"path/filepath"

	"archive/zip"

	"bufio"

	"github.com/linlexing/dbx/data"
	"github.com/linlexing/dbx/ddb"
	"github.com/linlexing/dbx/schema"
	"github.com/pborman/uuid"
	"github.com/robfig/cron"
)

const batchNum = 500

var (
	jobs    = cron.New()
	running = false
	jobRun  = &sync.Mutex{}
)

func taskRun() {
	if running {
		return
	}
	running = true
	defer func() {
		running = false
	}()
	dlog.Println("before lock")
	jobRun.Lock()
	defer jobRun.Unlock()
	dlog.Println("start job")
	err := buildDataFile()
	if err != nil {
		dlog.Error(err)
		return
	}
	//然后开始上传
	//uploadAll()

	dlog.Println("job finished")
}

//创建zip文件，并将templa文件夹中的文件复制到zip文件中
func createNewZipFile() (*os.File, *zip.Writer, error) {
	//添加在workDir后添加out路径
	outPath := filepath.Join(workDir, "out")
	//使用指定的权限和名称创建一个目录，包括任何必要的上级目录，并返回nil，否则返回错误
	if err := os.MkdirAll(outPath, os.ModePerm); err != nil {
		return nil, nil, err
	}
	//确定文件名过程为：
	//out目录中没有同名文件
	//upload目录中也没有同名文件
	var fileName string
	for i := 1; ; i++ {
		fileName = fmt.Sprintf("gsdata_%s_%s_%06d.zip", time.Now().Format("20060102"),
			vconfig.AreaCode, i)
		var not1, not2 bool
		//Stat返回一个描述name指定的文件对象的FileInfo。
		//如果指定的文件对象是一个符号链接，返回的FileInfo描述该符号链接指向的文件的信息，
		//本函数会尝试跳转该链接
		//IsNotExist返回一个布尔值说明该错误是否表示一个文件或目录不存在
		if _, err := os.Stat(filepath.Join(workDir, vconfig.FinishOut, fileName)); os.IsNotExist(err) {
			not1 = true
		} else if err != nil {
			return nil, nil, err
		}
		if _, err := os.Stat(filepath.Join(workDir, "out", fileName)); os.IsNotExist(err) {
			not2 = true
		} else if err != nil {
			return nil, nil, err
		}
		if not1 && not2 {
			break
		}
	}
	//创建文件名为fileName的zip
	file, err := os.Create(filepath.Join(outPath, fileName))
	if err != nil {
		return nil, nil, err
	}
	//得到一个将zip文件写入file的*Writer
	zipw := zip.NewWriter(file)
	//先复制模板文件
	//返回template指定的目录的目录信息的有序列
	files, err := ioutil.ReadDir(filepath.Join(workDir, "template"))
	if err != nil {
		return nil, nil, err
	}
	//将template文件夹里的文件复制到Zip中
	for _, f := range files {
		//使用给出的文件名添加一个文件进zip文件。本方法返回的w是一个io.Writer接口（用于写入新添加文件的内容）
		w, err := zipw.Create(f.Name())
		if err != nil {
			return nil, nil, err
		}
		//ReadFile 从filename指定的文件中读取数据并返回文件的内容
		bys, err := ioutil.ReadFile(filepath.Join(workDir, "template", f.Name()))
		if err != nil {
			return nil, nil, err
		}
		//通过w向文件中写入bys
		if _, err = w.Write(bys); err != nil {
			return nil, nil, err
		}
	}
	//w, err := zipw.Create("ent_info.dat")  bufio.NewWriter(w),

	//bufio.NewWriter创建一个具有默认大小缓冲、写入w的*Writer。
	return file, zipw, err
}

//打开主表和对应对的影子表
func openDB(fieldSize []int, tableName, shadowTableName, ID string) (ddb.DB, *data.Table, *data.Table, error) {
	db, err := ddb.Openx(vconfig.Driver, vconfig.DBURL)
	if err != nil {
		return nil, nil, nil, err
	}
	tab, err := data.OpenTable(db.DriverName(), db, tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	//必须全部是string类型
	for _, col := range tab.Columns {
		if col.Type != schema.TypeString {
			return nil, nil, nil, fmt.Errorf("column %s type not is string", col.Name)
		}
	}
	//配置文件里的字段没有连接字段，在此加上
	size := len(fieldSize) + 1
	if size != len(tab.Columns) {
		return nil, nil, nil, fmt.Errorf("field size %d <> column length %d", size, len(tab.Columns))
	}
	tab.Name = shadowTableName
	tab.PrimaryKeys = []string{strings.ToUpper(ID)}
	//自动更新影子表的结构
	if err = tab.Table.Update(db.DriverName(), db); err != nil {
		return nil, nil, nil, err
	}
	tab.Name = tableName
	shadowTable, err := data.OpenTable(db.DriverName(), db, shadowTableName)
	if err != nil {
		return nil, nil, nil, err
	}
	return db, tab, shadowTable, nil
}

func searchTable(maxrows int, ID string, db ddb.DB, tab, shadowTable *data.Table, tableField []int, maintable []detailTab, cb func(icount int,
	allRow [][]string) error) error {
	saveToShadowTable := func(diffRows [][]interface{}) error {
		//保存到影子表中
		for _, line := range diffRows {
			row := map[string]interface{}{}
			for i, col := range shadowTable.Columns {
				row[col.Name] = line[i]
			}
			if err := shadowTable.Save(row); err != nil {
				return err
			}
		}
		return nil
	}
	rows, err := db.Query(fmt.Sprintf("select %s from %s", ID, tab.Name))
	if err != nil {
		return err
	}
	defer rows.Close()
	//差异比较的数据量
	icount := 1
	//上传数据的数量
	rowsNum := 0
	pks := []interface{}{}
	for rows.Next() {
		var pk interface{}
		//得到每一行的第一个，即主键的值
		if err = rows.Scan(&pk); err != nil {
			return err
		}
		pks = append(pks, pk)
		if icount%batchNum == 0 {
			diffRows, allRows, err := queryDiff(db, tab, shadowTable.Name, tableField, maintable, pks)
			if err != nil {
				return err
			}

			num := int(len(diffRows))
			//主表的数量
			rowsNum += num
			pks = nil
			if err := cb(num, allRows); err != nil {
				return err
			}
			if err := saveToShadowTable(diffRows); err != nil {
				return err
			}

		}
		icount++
		//限制每次上传数据的数量
		if rowsNum >= maxrows {
			break
		}
	}
	if len(pks) > 0 {
		diffRows, allRow, err := queryDiff(db, tab, shadowTable.Name, tableField, maintable, pks)
		if err != nil {
			return err
		}
		num := int(len(diffRows))
		//主表的数量
		rowsNum += num
		pks = nil
		if err := cb(num, allRow); err != nil {
			return err
		}
		if err := saveToShadowTable(diffRows); err != nil {
			return err
		}
	}
	return nil
}

//向文件中写入内容
func writeLine(w *bufio.Writer, strs []string) error {
	for _, str := range strs {
		if _, err := w.WriteString(str); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return nil
}

//向文件中写入数据
func buildDataFile() error {
	file, zipw, err := createNewZipFile()
	if err != nil {
		log.Println(err)
		return err
	}
	defer file.Close()
	defer zipw.Close()

	//打开主表，并对数据进行简单处理
	tables := vconfig.Table
	for _, maintable := range tables {
		//创建dat文件，并得到一个向该文件写的流
		datFile, err := zipw.Create(fmt.Sprint(maintable.Name, ".dat"))
		if err != nil {
			log.Println(err)
			return err
		}
		//得到一个向dat文件写的缓冲流
		datw := bufio.NewWriter(datFile)
		defer datw.Flush()
		/*打开对应的主表*/
		db, tab, shadowTable, err := openDB(maintable.FieldSize, maintable.Name, maintable.Shadowtable, maintable.ID)
		if err != nil {
			log.Println(err)
			return err
		}
		icount := 0
		if err := searchTable(maintable.Maxrows, maintable.ID, db, tab, shadowTable, maintable.FieldSize,
			maintable.Details, func(i int, rows [][]string) error {
				if len(rows) > 0 {
					icount += i
					dlog.Println("rownum:", i, "write", i, "rows,total:", icount)
				}
				for _, line := range rows {
					if err := writeLine(datw, line); err != nil {
						log.Println(err)
						return err
					}
				}

				return nil
			}); err != nil {
			return err
		}
		//创建审核结果文件和审核状态文件
		_, err = zipw.Create(fmt.Sprint(maintable.Result, ".dat"))
		if err != nil {
			log.Println(err)
			return err
		}
		_, err = zipw.Create(fmt.Sprint(maintable.Status, ".dat"))
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

//queryDiff2 返回一个新增、变更的记录内容
func queryDiff(db ddb.DB, table *data.Table, shadowtable string, tableField []int, maintable []detailTab,
	pkvalues []interface{}) ([][]interface{}, [][]string, error) {
	str := fmt.Sprintf("%s in(?)", table.PrimaryKeys[0])
	where, params, err := data.In(str, pkvalues)
	if err != nil {
		return nil, nil, err
	}
	//两个表的where
	params = append(params, params...)
	strSQL := data.Find(db.DriverName()).Minus(table.FullName(), where,
		shadowtable, where, table.PrimaryKeys, table.ColumnNames)
	//log.Println("strsql ", strSQL)
	rows, err := db.Query(strSQL, params...)

	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	//转换主表信息的格式
	rev, err := trunRows(rows)
	if err != nil {
		return nil, nil, err
	}
	//存放子表的查询信息
	sonMap := make(map[string][][]interface{})
	//子表字段长度集
	sonFeild := make(map[string][]int)
	//判断主表是否有数据变动，如有,则对子表进行查询
	if len(rev) > 0 {
		//得到需要查询子表的主键值
		sonPK := make([]interface{}, len(rev))
		for i, value := range rev {
			sonPK[i] = value[0]
		}
		//查询所有子表
		for _, sonTable := range maintable {
			strSon := fmt.Sprintf("%s in(?)", sonTable.PreID)
			if err != nil {
				return nil, nil, err
			}
			where, params, err := data.In(strSon, sonPK)
			if err != nil {
				return nil, nil, err
			}
			sonRows, err := db.Query(fmt.Sprintf("select /*+parallel(4)*/* from %s where %s ", sonTable.Name, where), params...)
			if err != nil {
				return nil, nil, err
			}
			defer sonRows.Close()
			//转换查出的子表信息
			sonRow, err := trunRows(sonRows)
			if err != nil {
				return nil, nil, err
			}
			sonMap[sonTable.Name] = sonRow
			sonFeild[sonTable.Name] = sonTable.Sizes
		}
	}

	//添加信息
	dateTimeFields := []int{}
	for i, col := range table.Columns {
		switch col.Name {
		case "数据上传时间":
			dateTimeFields = append(dateTimeFields, i)
		}
	}
	//复制主表差异记录，写入影子表，防止因修改写入字段值所产生的混淆。
	revShadow := [][]interface{}{}
	//存放组合后的数据
	allLine := [][]string{}
	for _, newLine := range rev {
		shadowLine := make([]interface{}, len(newLine))
		copy(shadowLine, newLine)
		revShadow = append(revShadow, shadowLine)
		//设置主表uuid的值
		newLine[2] = interface{}(hex.EncodeToString(uuid.NewUUID()))
		//设置时间字段
		for _, idx := range dateTimeFields {
			newLine[idx] = time.Now().Format("20060102150405")
		}
		//字段与Field不对应
		str, err := alterLine(tableField, newLine[1:])
		if err != nil {
			return nil, nil, err
		}
		allLine = append(allLine, str)
		//查询子表
		for sonTableName, sonrow := range sonMap {
			for _, line := range sonrow {
				//根据主表与子表的连接字段，查询每个子表
				if strings.EqualFold(line[0].(string), newLine[0].(string)) {
					//设置子表中的子表Guid字段
					line[2] = newLine[2]
					str, err := alterLine(sonFeild[sonTableName], line[1:])
					if err != nil {
						return nil, nil, err
					}
					allLine = append(allLine, str)
				}
			}
		}
	}
	return revShadow, allLine, nil
}

//修改字段的相关要求，给不够长度的字段添加空格
func alterLine(FieldSize []int, data []interface{}) ([]string, error) {
	writeLine := []string{}
	for i, v := range data {
		var str string
		switch tv := v.(type) {
		case string:
			str = tv
		case []byte:
			str = string(tv)
		case nil:
		default:
			return nil, fmt.Errorf("%T not in string", v)
		}
		str = strings.Replace(
			strings.Replace(str, "\r", " ", -1),
			"\n", " ", -1)
		//得到str的字符长度，而不是字节
		rstr := []rune(str)
		if len(rstr) < FieldSize[i] {
			str = str + strings.Repeat(" ", FieldSize[i]-len(rstr))
		} else if len(rstr) > FieldSize[i] {
			str = string(rstr[:FieldSize[i]])
		}
		writeLine = append(writeLine, str)
	}
	return writeLine, nil
}

func trunRows(rows *sql.Rows) ([][]interface{}, error) {
	rev := [][]interface{}{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colCount := len(cols)
	for rows.Next() {
		row := make([]interface{}, colCount)
		for i := range row {
			row[i] = new(interface{})
		}
		if err = rows.Scan(row...); err != nil {
			return nil, err
		}
		line := make([]interface{}, colCount)
		for i := range row {
			line[i] = *(row[i].(*interface{}))
		}
		rev = append(rev, line)
	}
	return rev, nil
}
