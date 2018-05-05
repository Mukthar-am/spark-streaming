package org.muks.insider.cli;

import java.util.List;

/**
 * Created by 15692 on 16/08/16. Parser options class to store options list
 */

public class ParserOptions {
    List<ParserOptionEntity> optionsList;

    public ParserOptions(List<ParserOptionEntity> optionsList) {
        this.optionsList = optionsList;
    }

    public List<ParserOptionEntity> getOptionsList() {
        return optionsList;
    }
}
