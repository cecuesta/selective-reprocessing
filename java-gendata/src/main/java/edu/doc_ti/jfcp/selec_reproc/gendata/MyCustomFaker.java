package edu.doc_ti.jfcp.selec_reproc.gendata;

import net.datafaker.providers.base.BaseFaker;

public class MyCustomFaker extends BaseFaker {

    public MyElements MyElements() {
        return getProvider(MyElements.class, MyElements::new, this);
    }


    
}

