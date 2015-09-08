package net.wouto.modelsync.postgresql.annotations;

import net.wouto.modelsync.postgresql.persistence.IndexSetting;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Requires an {@link net.wouto.modelsync.postgresql.persistence.IndexSetting} value
 *
 * @author Wouter
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD })
public @interface Index {

    public IndexSetting value();

}
