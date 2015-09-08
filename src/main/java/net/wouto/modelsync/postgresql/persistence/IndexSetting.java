package net.wouto.modelsync.postgresql.persistence;

public enum IndexSetting {
    /**
     * If AUTO_INCREMENT is chosen, the SQLModelManager will not attempt to insert this value
     */
    AUTO_INCREMENT,
    /**
     * If PROVIDED is chosen, the SQLModelManager will write a manual index
     */
    PROVIDED
}
