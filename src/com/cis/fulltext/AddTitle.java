package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;


/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: 4/8/12
 * Time: 11:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class AddTitle {
    public static void main(String[] args) throws Exception {
        Env.homePath = "/cndata/recent_index/WEB-INF/";
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();

        UnionIndex unionIndex = UnionIndex.getInstance();
        int from = 0;
        if(args.length == 1) {
            from = Integer.parseInt(args[0]);
        }
        unionIndex.getAllRecords(from);
    }
}
