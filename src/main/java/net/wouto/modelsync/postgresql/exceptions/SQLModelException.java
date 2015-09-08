package net.wouto.modelsync.postgresql.exceptions;

import net.wouto.modelsync.postgresql.persistence.SQLModelManager;

public class SQLModelException extends Exception {
    
    public SQLModelException(SQLModelManager manager, String message) {
        super("Error in SQLModel '" + manager.getModelClass().getName() + "': " + message);
    }
    
}
