package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;

/**
 * Created by IntelliJ IDEA.
 * User: luhang
 * Date: 5/26/11
 * Time: 4:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class Delete {
    public static void main(String[] args) {
        Env.homePath = args[0];
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        Index idx = new Index(args[1]);

    }
}
