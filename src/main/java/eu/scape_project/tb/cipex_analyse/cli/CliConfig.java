/*
 *  Copyright 2012 The SCAPE Project Consortium.
 * 
 *  Licensed under the Apache License; Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing; software
 *  distributed under the License is distributed on an "AS IS" BASIS;
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.scape_project.tb.cipex_analyse.cli;

/**
 * Process configuration 
 * @author Sven Schlarb https://github.com/shsdev
 * @version 0.1
 */
public class CliConfig implements Cloneable {
  
    private String dirStr;
  
    /**
     * Empty constructor
     */
    public CliConfig() {
        
    }
    
    /**
     * Getter for the directories parameter
     * @return Directories parameter
     */
    public String getDirStr() {
        return dirStr;
    }

    /**
     * Setter for the directories parameter
     * @param dirStr Directories parameter
     */
    public void setDirStr(String dirsStr) {
        this.dirStr = dirsStr;
    }
    
    /**
     * Clone object
     * @return cloned object
     */
    @Override public CliConfig clone() {
        try {
            return (CliConfig) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new AssertionError();
        }
    }
    
}
