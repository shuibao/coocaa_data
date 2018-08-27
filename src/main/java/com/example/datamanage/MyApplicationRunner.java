package com.example.datamanage;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.mysql.jdbc.PreparedStatement;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;

@Component
public class MyApplicationRunner implements ApplicationRunner {

    //static String infile = "C:/Users/lxq/Desktop/coocaa/data/08071883/Project Manage-CSV.csv";
    //static String filterOutFile = "C:/Users/lxq/Desktop/coocaa/data/08071883/demores.csv";
    //static String illegalOutFile = "C:/Users/lxq/Desktop/coocaa/data/08071883/notused.csv";

    static int illegalDataNum = 0;
    static int allNum = 0;
    static int usedNum = 0;
    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.driver-class-name}")
    private String name;
    @Value("${spring.datasource.username}")
    private String user;
    @Value("${spring.datasource.password}")
    private String password;
    public void run(ApplicationArguments args) throws Exception {
        File path = new File(ResourceUtils.getURL("classpath:").getPath());
        if(!path.exists()) path = new File("");
        //System.out.println("path:"+path.getAbsolutePath());
        String infileName ="";
        if(args!=null&&args.getSourceArgs().length!=0){
            infileName = args.getSourceArgs()[0];
        }
//        String infile =path.getAbsolutePath()+"\\"+"data.csv";
//        String illegalOutFile = path.getAbsolutePath()+"\\"+"notused.csv";
        String infile = "C:/Users/lxq/Desktop/coocaa/data/08231006/Project Manage.csv";
        String filterOutFile = "C:/Users/lxq/Desktop/coocaa/data/08231006/filter_data.csv";
        String illegalOutFile = "C:/Users/lxq/Desktop/coocaa/data/08231006/illegal_data.csv";
        Connection conn = null;
        Class.forName(name);//指定连接类型
        conn = DriverManager.getConnection(url, user, password);//获取连接

        ArrayList<ArrayList>lists = new ArrayList<>();
//         开始时间
        Long begin = new Date().getTime();
        if (conn!=null) {
            //System.out.println("获取连接成功");
            lists = readCsv(infile,illegalOutFile,filterOutFile);
            insertIssue(conn,lists.get(0));
            //project数据表 此表作用不大，改动也比较小，如果每次都更新的话会降低性能，
            insertProject(conn,lists.get(1));
            Long end = new Date().getTime();
            DecimalFormat df = new DecimalFormat("0.00");
            String passingRate = df.format((float)usedNum*100/(usedNum+illegalDataNum));
            System.out.print("已入库数据 :" + usedNum+ " 条");
            System.out.print("  非法数据 :" + illegalDataNum + " 条");
            System.out.print("  过滤数据 :" + (allNum-usedNum-illegalDataNum) + " 条");
            System.out.println("  总数据 :" + allNum + " 条");
            System.out.println("数据合格率 :" + passingRate+"%");
            System.out.println("花费时间 :" +(float)(end-begin)/1000+"s" );
        }else {
            System.out.println("获取连接失败");
        }


    }
    public static ArrayList<ArrayList> readCsv(String infile,String illegalOutFile,String filterOutFile ){
        // 用来保存CSV里面的所有数据（未筛选）
        ArrayList<String[]> csvFileList = new ArrayList<String[]>();
        //用来保存筛选后的合法数据
        ArrayList<String[]> issueList = new ArrayList<>();
        ArrayList<String[]> projectList = new ArrayList<>();
        ArrayList<ArrayList>resList = new ArrayList<>();
        // 创建CSV读对象 例如:CsvReader(文件路径，分隔符，编码格式);
        try {
            CsvReader reader = new CsvReader(infile, ',', Charset.forName("GBK"));
            //不合理的数据输出打印为CSV
            CsvWriter illegalDataWriter = new CsvWriter(illegalOutFile, ',', Charset.forName("GBK"));
            CsvWriter filterDataWriter = new CsvWriter(filterOutFile, ',', Charset.forName("GBK"));
            int indexs[] = new int[27];
            for(int i = 0; i < indexs.length; i++) {
                indexs[i] = -1;
            }

            Boolean ret = reader.readHeaders();
            //从CSV里面或许需要字段的数据
            if(ret) {
                String headers[] = reader.getHeaders();
                //System.out.println(headers[22]+" "+headers[23]+" "+headers[24]);
                for(int i = 0; i < headers.length; i++)
                {
                    //System.out.println(headers[i]);
                    if(headers[i].toLowerCase().contains("summary")) {
                        indexs[0] = i;
                    }

                    if(headers[i].toLowerCase().contains("issue key")) {
                        indexs[1] = i;
                    }

                    if(headers[i].toLowerCase().contains("issue id")) {
                        indexs[2] = i;
                    }

                    if(headers[i].toLowerCase().contains("issue type")) {
                        indexs[3] = i;
                    }

                    if(headers[i].toLowerCase().equals("status")) {
                        indexs[4] = i;
                    }

                    if(headers[i].toLowerCase().contains("project key")) {
                        indexs[5] = i;
                    }

                    if(headers[i].toLowerCase().contains("project name")) {
                        indexs[6] = i;
                    }

                    if(headers[i].toLowerCase().contains("project type")) {
                        indexs[7] = i;
                    }

                    if(headers[i].toLowerCase().contains("project lead")) {
                        indexs[8] = i;
                    }

                    if(headers[i].toLowerCase().contains("priority")) {
                        indexs[9] = i;
                    }

                    if(headers[i].toLowerCase().contains("resolution")) {
                        indexs[10] = i;
                    }

                    if(headers[i].toLowerCase().contains("assignee")) {
                        indexs[11] = i;
                    }

                    if(headers[i].toLowerCase().contains("reporter")) {
                        indexs[12] = i;
                    }

                    if(headers[i].toLowerCase().contains("creator")) {
                        indexs[13] = i;
                    }

                    if(headers[i].toLowerCase().contains("created")) {
                        indexs[14] = i;
                    }

                    if(headers[i].toLowerCase().contains("updated")) {
                        indexs[15] = i;
                    }

                    if(headers[i].toLowerCase().contains("last viewed")) {
                        indexs[16] = i;
                    }

                    if(headers[i].toLowerCase().contains("resolved")) {
                        indexs[17] = i;
                    }

                    if(headers[i].toLowerCase().equals("original estimate")) {
                        indexs[18] = i;
                    }

                    if(headers[i].toLowerCase().equals("remaining estimate")) {
                        indexs[19] = i;
                    }

                    if(headers[i].toLowerCase().contains("time spent")) {
                        indexs[20] = i;
                    }

                    if(headers[i].toLowerCase().contains("work ratio")) {
                        indexs[21] = i;
                    }

                    if(headers[i].equals("Σ Original Estimate")) {
                        indexs[22] = i;
                    }

                    if(headers[i].equals("Σ Remaining Estimate")) {
                        indexs[23] = i;
                    }

                    if(headers[i].equals("Σ Time Spent")) {
                        indexs[24] = i;
                    }

                    if(headers[i].toLowerCase().contains("sprint")) {
                        indexs[25] = i;
                    }
                    //斐波那契数列
                    if(headers[i].toLowerCase().contains("story points")) {
                        indexs[26] = i;
                    }
                }
            }            // 逐行读入除表头的数据
            while (reader.readRecord()) {
                //System.out.println(reader.getRawRecord());
                csvFileList.add(reader.getValues());
            }
            // 写表头
            //String[] csvHeaders = { "编号", "姓名", "年龄" };
            String[]illegalDataHeaders ={"Summary","Issue key","Issue id","Issue Type","Status","Project key","Project name","Project type","Project lead","Priority","Resolution","Assignee","Reporter","Creator","Created","Updated","Last Viewed","Resolved","Original Estimate","Remaining Estimate","Time Spent","Work Ratio","Σ Original Estimate","Σ Remaining Estimate","Σ Time Spent","Sprint","Custom field (Story Points)"};
            illegalDataWriter.writeRecord(illegalDataHeaders);

            reader.close();
            // 写内容
            // 遍历读取的CSV文件
            //不合格数据数量

            //总数量
            allNum = csvFileList.size();
            for (int row = 0; row < csvFileList.size(); row++) {
                // 取得第row行第0列的数据
                //0,1,2,4,5,6,7,8,9,12,13,14,15,16,17,18,19,20,29,30,31,32,33,34,35,44,45
                //String cell = csvFileList.get(row)[0];
                //System.out.println("------------>"+cell);

                //if(csvFileList.get(row)[])
                String[] csvContent = {csvFileList.get(row)[indexs[0]], csvFileList.get(row)[indexs[1]], csvFileList.get(row)[indexs[2]], csvFileList.get(row)[indexs[3]], csvFileList.get(row)[indexs[4]],
                        csvFileList.get(row)[indexs[5]], csvFileList.get(row)[indexs[6]], csvFileList.get(row)[indexs[7]], csvFileList.get(row)[indexs[8]], csvFileList.get(row)[indexs[9]],
                        csvFileList.get(row)[indexs[10]], csvFileList.get(row)[indexs[11]], csvFileList.get(row)[indexs[12]], csvFileList.get(row)[indexs[13]], csvFileList.get(row)[indexs[14]],
                        csvFileList.get(row)[indexs[15]], csvFileList.get(row)[indexs[16]], csvFileList.get(row)[indexs[17]], csvFileList.get(row)[indexs[18]], csvFileList.get(row)[indexs[19]],
                        csvFileList.get(row)[indexs[20]], csvFileList.get(row)[indexs[21]], csvFileList.get(row)[indexs[22]], csvFileList.get(row)[indexs[23]], csvFileList.get(row)[indexs[24]], csvFileList.get(row)[indexs[25]],
                        csvFileList.get(row)[indexs[26]]};
                String[] csvIssueContent = {csvContent[0],csvContent[6], csvContent[1], csvContent[2], csvContent[3], csvContent[4], csvContent[9],
                        csvContent[10], csvContent[11], csvContent[12], csvContent[13], csvContent[14],
                        csvContent[15], csvContent[16], csvContent[17], csvContent[18], csvContent[19],
                        csvContent[20], csvContent[21], csvContent[22], csvContent[23], csvContent[24], csvContent[25],
                        csvContent[26]};

                if(csvFileList.get(row)[indexs[4]].equals("Closed")||csvFileList.get(row)[indexs[4]].equals("Resolved")){
                    csvIssueContent[5]="1";
                }else if(csvFileList.get(row)[indexs[4]].equals("In Progress")){
                    csvIssueContent[5]="0";
                }else if(csvFileList.get(row)[indexs[4]].equals("New")||csvFileList.get(row)[indexs[4]].equals("Open")){
                    csvIssueContent[5]="-1";
                }else{
                    csvIssueContent[5]="-2";
                }
                //数据筛选

                //issue type 为Bug,Epic,Sub-task,Task 过滤掉
                if(csvContent[3].equals("Bug")||csvContent[3].equals("Epic")||csvContent[3].equals("Sub-task")||csvContent[3].equals("Task")){
                    filterDataWriter.writeRecord(csvContent);
                }
                //issue type 为Story,Improvement,New Feature则筛选
                if(csvContent[3].equals("Story")||csvContent[3].equals("Improvement")||csvContent[3].equals("New Feature")) {
                    //issue status为new和open 如果sprint为空则story point 可以为空或者满足要求
                    if (csvIssueContent[5].equals("-1")&&csvContent[25].equals("")){
                        if (!csvContent[26].equals("")){
                            if(csvContent[26].contains(".")||!isFibonacci(Integer.parseInt(csvContent[26]))){
                            illegalDataWriter.writeRecord(csvContent);
                            illegalDataNum++;}
                        }else{
                            filterDataWriter.writeRecord(csvContent);
                        }
                    }else{
                        //预估时间不能为空或者0
                        if(csvContent[18].equals("")||csvContent[18].equals("0")) {
                            illegalDataWriter.writeRecord(csvContent);
                            illegalDataNum++;
                        }
                        //工作量不能为空且满足斐波那契数列
                        else if (csvContent[26].equals("")||csvContent[26].contains(".")||!isFibonacci(Integer.parseInt(csvContent[26]))){
                            illegalDataWriter.writeRecord(csvContent);
                            illegalDataNum++;
                        }
                        //如果问题已解决则花费时间不能为空
                        else if(csvIssueContent[5].equals("1")&&csvContent[20].equals("")||csvContent[20].equals("0")){
                            illegalDataWriter.writeRecord(csvContent);
                            illegalDataNum++;
                        }else{
                            //datetime不能写入空
                            //如果resolved为空 将它置为0000/00/00 00:00
                            if(csvIssueContent[14].equals("")){
                                csvIssueContent[14]="1800/01/01 00:00";
                            }
                            //花费时间为空 则导入0
                            if(csvIssueContent[17].equals("")){
                                csvIssueContent[17]="0";
                            }
                            issueList.add(csvIssueContent);
                        }
                    }
                }
                //project table
                String[] csvProjectContent = {csvContent[5], csvContent[6], csvContent[7], csvContent[8]};

                projectList.add(csvProjectContent);
            }
            filterDataWriter.close();
            illegalDataWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        resList.add(issueList);
        resList.add(projectList);
        return resList;
    }
    public static void insertIssue(Connection conn, ArrayList<String[]> dataList) {

        // sql前缀
//        String prefix = "INSERT INTO issue (summary,issue_key,issue_id,issue_type,status,project_key," +
//                "project_name,project_type,project_lead,priority,resolution,assignee,reporter,creator," +
//                "created,updated,last_viewed,resolved,original_estimate,remaining_estimate,time_spent," +
//                "work_ratio,all_original_estimate,all_remaining_estimate,all_time_spent,sprint,story_point) VALUES ";
        String prefix = "INSERT INTO tb_issue (summary,project_name,issue_key,issue_id,issue_type,status,priority,resolution,assignee,reporter,creator, created,updated,last_viewed,resolved,original_estimate,remaining_estimate,time_spent,work_ratio,all_original_estimate,all_remaining_estimate,all_time_spent,sprint,story_point) VALUES ";
        try {
            // 保存sql后缀
            StringBuffer suffix = new StringBuffer();
            // 设置事务为非自动提交
            conn.setAutoCommit(false);
            // 比起st，pst会更好些
            PreparedStatement pst = (PreparedStatement) conn.prepareStatement("");//准备执行语句
            int dataNum = dataList.size();
            // 外层循环，总提交事务次数
            int k = dataNum/10000+1;
            int lastSize = dataNum%10000==0?10000:dataNum%10000;
            for (int i = 0; i <k ; i++) {
                int m = i==k-1?lastSize:10000;
                for (int row = 0; row < m; row++) {
                    suffix = new StringBuffer();
                    // 第j次提交步长
//                for (int j = 1; j <= 1000; j++) {
//                    // 构建SQL后缀
//                     suffix.append("('").append(UUID.randomUUID().toString()).append("','") .append(i*j).append("','123456','男','教师','www.bbk.com','XX大学','2016-08-12 14:43:26','备注'),");
//                }
//                suffix.append("('").append(dataList.get(row)[0]).append("','") .append(dataList.get(row)[1]).append("','").append(dataList.get(row)[2]).append("','")
//                        .append(dataList.get(row)[3]).append("','").append(dataList.get(row)[4]).append("','").append(dataList.get(row)[5]).append("','").append(dataList.get(row)[6]).append("','")
//                        .append(dataList.get(row)[7]).append("','").append(dataList.get(row)[8]).append("','").append(dataList.get(row)[9]).append("','").append(dataList.get(row)[10]).append("','")
//                        .append(dataList.get(row)[11]).append("','").append(dataList.get(row)[12]).append("','").append(dataList.get(row)[13]).append("','").append(dataList.get(row)[14]).append("','")
//                        .append(dataList.get(row)[15]).append("','").append(dataList.get(row)[16]).append("','").append(dataList.get(row)[17]).append("','").append(dataList.get(row)[18]).append("','")
//                        .append(dataList.get(row)[19]).append("','").append(dataList.get(row)[20]).append("','").append(dataList.get(row)[21]).append("','").append(dataList.get(row)[22]).append("','")
//                        .append(dataList.get(row)[23]).append("','").append(dataList.get(row)[24]).append("','").append(dataList.get(row)[25]).append("','").append(dataList.get(row)[26]).append("'),");
                    suffix.append("('").append(dataList.get(row)[0]).append("','") .append(dataList.get(row)[1]).append("','").append(dataList.get(row)[2]).append("','")
                            .append(dataList.get(row)[3]).append("','").append(dataList.get(row)[4]).append("','").append(dataList.get(row)[5]).append("','").append(dataList.get(row)[6]).append("','")
                            .append(dataList.get(row)[7]).append("','").append(dataList.get(row)[8]).append("','").append(dataList.get(row)[9]).append("','").append(dataList.get(row)[10]).append("','")
                            .append(dataList.get(row)[11]).append("','").append(dataList.get(row)[12]).append("','").append(dataList.get(row)[13]).append("','").append(dataList.get(row)[14]).append("','")
                            .append(dataList.get(row)[15]).append("','").append(dataList.get(row)[16]).append("','").append(dataList.get(row)[17]).append("','").append(dataList.get(row)[18]).append("','")
                            .append(dataList.get(row)[19]).append("','").append(dataList.get(row)[20]).append("','").append(dataList.get(row)[21]).append("','").append(dataList.get(row)[22]).append("','").append(dataList.get(row)[23]).append("'),");

                    // 构建完整SQL
                    String sql = prefix + suffix.substring(0, suffix.length() - 1)+"ON DUPLICATE KEY UPDATE issue_id=VALUES(issue_id)";
                    // 添加执行SQL
                    pst.addBatch(sql);
                    usedNum++;
                }
                // 执行操作
                pst.executeBatch();
                // 提交事务
                conn.commit();

                // 清空上一次添加的数据
                suffix = new StringBuffer();
            }
            // 头等连接
            pst.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void insertProject(Connection conn, ArrayList<String[]> dataList) {

        // sql前缀
        String prefix = "INSERT ignore INTO tb_project (project_key,project_name,project_type,project_lead) VALUES ";
        try {
            // 保存sql后缀
            StringBuffer suffix = new StringBuffer();
            // 设置事务为非自动提交
            conn.setAutoCommit(false);
            // 比起st，pst会更好些
            PreparedStatement  pst = (PreparedStatement) conn.prepareStatement("");//准备执行语句
            int dataNum = dataList.size();
            // 外层循环，总提交事务次数
            int k = dataNum/10000+1;
            int lastSize = dataNum%10000==0?10000:dataNum%10000;
            for (int i = 0; i <k ; i++) {
                int m = i==k-1?lastSize:10000;
                for (int row = 0; row < m; row++) {
                    suffix = new StringBuffer();
                    // 第j次提交步长
//                for (int j = 1; j <= 1000; j++) {
//                    // 构建SQL后缀
//                     suffix.append("('").append(UUID.randomUUID().toString()).append("','") .append(i*j).append("','123456','男','教师','www.bbk.com','XX大学','2016-08-12 14:43:26','备注'),");
//                }
                    suffix.append("('").append(dataList.get(row)[0]).append("','") .append(dataList.get(row)[1]).append("','").append(dataList.get(row)[2]).append("','").append(dataList.get(row)[3]).append("'),");

                    // 构建完整SQL
                    String sql = prefix + suffix.substring(0, suffix.length() - 1);
                    // 添加执行SQL
                    pst.addBatch(sql);
                }

                // 执行操作
                pst.executeBatch();
                // 提交事务
                conn.commit();
                // 清空上一次添加的数据
                suffix = new StringBuffer();
            }
            // 头等连接
            pst.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static boolean isFibonacci(int n){
        int i = 1 , f = 0;
        while (true){
            f =fibonacci(i);
            if(f == n) {return true;}
            if(f > n) return false;
            i++;
        }
    }
    public static int fibonacci(int n){
        if(n<=2)return 1;
        else return fibonacci(n-1)+fibonacci(n-2);
    }

}
