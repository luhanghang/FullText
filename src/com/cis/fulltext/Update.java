package com.cis.fulltext;

import com.fb.common.util.Env;
import com.fb.db.AllIndexsInfo;

public class Update {
    public static void main(String[] args) throws Exception {
        Env.homePath = args[0];
        AllIndexsInfo.getInstance().loadIndexInfoFromDB();
        System.out.println(Env.homePath);
        Index idx = new Index();
        if(args.length == 2) {
            idx.update(args[1]);
        } else {
            idx.update();
        }
    }
}
