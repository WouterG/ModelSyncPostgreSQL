package net.wouto.modelsync.postgresql.persistence;

import net.wouto.modelsync.postgresql.PostgreSQLConnection;
import net.wouto.modelsync.postgresql.annotations.CalculatedColumn;
import net.wouto.modelsync.postgresql.annotations.Column;
import net.wouto.modelsync.postgresql.annotations.Index;
import net.wouto.modelsync.postgresql.annotations.Table;
import net.wouto.modelsync.postgresql.exceptions.SQLModelException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.ClassUtils;
import org.jooq.Batch;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Param;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.UpdateQuery;
import org.jooq.impl.DSL;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.val;

public class SQLModelManager<T> {

    private final Class<T> type;
    private final DSLContext create;
    private final Table table;
    private final Map<Field, Column> fields;
    private final Map<Method, CalculatedColumn> calculatedFields;
    private String indexKey;
    private Field indexField;
    private Method indexMethod;
    private IndexSetting indexSetting;

    private Connection connection;

    public SQLModelManager(PostgreSQLConnection connection) throws Exception {
        this.connection = connection.getConnection();
        this.type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.create = DSL.using(this.connection, SQLDialect.POSTGRES_9_4);

        if (this.type.isAnnotationPresent(Table.class)) {
            this.table = this.type.getAnnotation(Table.class);
        } else {
            throw new SQLModelException(this, "No @Table annotation present.");
        }

        try {
            this.createEmptyInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new SQLModelException(this, "No constructor without arguments was found");
        }

        this.fields = new LinkedHashMap();
        this.calculatedFields = new LinkedHashMap();

        for (Method m : this.type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(CalculatedColumn.class)) {
                if (m.getReturnType() == null || m.getParameterTypes().length > 0) {
                    continue;
                }
                CalculatedColumn cc = m.getAnnotation(CalculatedColumn.class);
                m.setAccessible(true);
                this.calculatedFields.put(m, cc);
                if (m.isAnnotationPresent(Index.class)) {
                    if (indexField != null || indexMethod != null) {
                        throw new SQLModelException(this, "Model has more than one @Index. Please use a single - never changing - identifier, like id");
                    } else {
                        indexMethod = m;
                        this.indexKey = cc.value();
                        this.indexSetting = m.getAnnotation(Index.class).value();
                    }
                }
            }
        }
        for (Field f : this.type.getDeclaredFields()) {
            if (f.isAnnotationPresent(Column.class)) {
                f.setAccessible(true);
                Column c = f.getAnnotation(Column.class);
                this.fields.put(f, c);
                if (f.isAnnotationPresent(Index.class)) {
                    if (indexField != null || indexMethod != null) {
                        throw new SQLModelException(this, "Model has more than one @Index. Please use a single - never changing - identifier, like id");
                    } else {
                        indexField = f;
                        this.indexKey = ((c.value() == null || c.value().isEmpty()) ? f.getName() : c.value());
                        this.indexSetting = f.getAnnotation(Index.class).value();
                    }
                }
            }
        }
        if (indexField == null && indexMethod == null) {
            throw new SQLModelException(this, "Model has no @Index specified");
        }
    }

    /**
     * This is a really stupid function, bemac if you call this on accident,
     * don't blame me
     *
     * @param areYouSure0 make sure this is false
     * @param areYouSure1 make sure this is false
     * @param areYouSure2 make sure this is false
     * @param areYouSure3 make sure this is false
     * @param areYouSure4 make sure this is false
     * @param areYouSure5 make sure this is false
     * @param areYouSure6 make sure this is false
     * @param areYouSure7 make sure this is false
     * @param areYouSure8 make sure this is false
     * @param areYouSure9 make sure this is false
     * @return whether or not the table was truncated entirely
     */
    public boolean deleteAll(boolean areYouSure0, boolean areYouSure1, boolean areYouSure2,
            boolean areYouSure3, boolean areYouSure4, boolean areYouSure5, boolean areYouSure6,
            boolean areYouSure7, boolean areYouSure8, boolean areYouSure9) {
        if (!areYouSure0 || !areYouSure1 || !areYouSure2 || !areYouSure3 || !areYouSure4
                || !areYouSure5 || !areYouSure6 || !areYouSure7 || !areYouSure8 || !areYouSure9) {
            return false;
        }
        this.create.truncate(this.table.value()).execute();
        System.out.println("Fuck you bemac");
        return true;
    }

    /**
     * Delete multiple items from a database
     *
     * @param items the items to delete from the database
     * @return affected row count
     */
    public int deleteMany(Collection<T> items) {
        int c = 0;
        for (T t : items) {
            if (deleteOne(t)) {
                c++;
            }
        }
        return c;
    }

    /**
     * Delete an instance from the database
     *
     * @param instance the instance you want to destroy
     * @return
     */
    public boolean deleteOne(T instance) {
        try {
            org.jooq.Table<Record> tbl = DSL.table(this.table.value());
            Entry<org.jooq.Field<Object>, Object> index = getIndex(instance);
            int edited = this.create.delete(tbl).where(index.getKey().eq(index.getValue())).execute();
            return (edited > 0);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(SQLModelManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * Delete record(s) where the specified key matches the given value
     *
     * @param key key to search for
     * @param value value you think key has
     * @return how many rows were affected
     */
    public int delete(String key, Object value) {
        org.jooq.Table<Record> tbl = DSL.table(this.table.value());
        int d = this.create.delete(tbl).where(field(key).eq(value)).execute();
        return d;
    }

    /**
     * Delete record(s) where the condition is valid
     *
     * @param condition the condition
     * @return the amount of affected records
     */
    public int delete(Condition condition) {
        org.jooq.Table<Record> tbl = DSL.table(this.table.value());
        int d = this.create.delete(tbl).where(condition).execute();
        return d;
    }

    /**
     * Returns the amount of successfully saved documents
     *
     * @param instances instances to save
     * @return amount of successfully saved documents
     */
    public int saveMany(Collection<T> instances) {
//        int sc = 0;
//        for (T i : instances) {
//            if (save(i)) {
//                sc++;
//            }
//        }
//        return sc;
        if (instances == null || instances.isEmpty()) {
            return -1;
        }
        Iterator<T> it = instances.iterator();
        List<Query> queries = new ArrayList();
        while (it.hasNext()) {
            T t = it.next();
            Entry<org.jooq.Field<Object>, Object> indexSet = getIndex(t);
            org.jooq.Table<Record> tbl = DSL.table(this.table.value());

            Map<org.jooq.Field<Object>, Object> data = getSerializableObject(t);

            List<Param<?>> params = new LinkedList();
            List<org.jooq.Field<?>> fields = new LinkedList();
            for (Entry<org.jooq.Field<Object>, Object> e : data.entrySet()) {
                params.add(val(e.getValue()));
                fields.add(e.getKey());
            }

            SelectConditionStep notExistsSelect = this.create.selectOne().from(tbl).where(indexSet.getKey().eq(indexSet.getValue()));
            SelectConditionStep insertIntoSelect = this.create.select(params).whereNotExists(notExistsSelect);

            Query update = this.create.update(tbl).set(data).where(indexSet.getKey().eq(indexSet.getValue()));
            Query insert = this.create.insertInto(tbl, fields).select(insertIntoSelect);
            queries.add(update);
            queries.add(insert);
        }
        Batch b = this.create.batch(queries);
        int[] d = b.execute();
        int j = 0;
        for (int k : d) {
            j += k;
        }
        return j;
    }

    /**
     * Save document to the database
     *
     * @param instance the document to save
     * @return whether or not the save was successful
     */
    public boolean save(T instance) {
        try {
            if (instance == null) {
                return false;
            }
            Entry<org.jooq.Field<Object>, Object> indexSet = getIndex(instance);
            org.jooq.Table<Record> tbl = DSL.table(this.table.value());
            Record r = this.create.select().from(this.table.value()).where(indexSet.getKey().eq(indexSet.getValue())).fetchOne();
            if (r == null) {
                Map<org.jooq.Field<Object>, Object> data = getSerializableObject(instance);
                this.create.insertInto(tbl).columns(data.keySet()).values(data.values()).execute();
                return true;
            } else {
                UpdateQuery u = this.create.updateQuery(tbl);
                Map<org.jooq.Field<Object>, Object> data = getSerializableObject(instance);
                for (Entry<org.jooq.Field<Object>, Object> e : data.entrySet()) {
                    u.addValue(e.getKey(), e.getValue());
                }
                u.addConditions(indexSet.getKey().eq(indexSet.getValue()));
                u.execute();
                return true;
            }
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(SQLModelManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * Find all documents in the database
     *
     * @return list of all documents in the database
     */
    public List<T> findAll() {
        Result<Record> result = this.create.select().from(this.table.value()).fetch();
        List<T> output = new ArrayList();
        for (Record r : result) {
            T t = recordToObject(r);
            if (t != null) {
                output.add(t);
            }
        }
        return output;
    }

    /**
     * Find a single document from the database, where the key-value pair
     * matches.
     *
     * @param fieldName the key of the search
     * @param value the value of the search
     * @return a single document or null if none was found.
     */
    public T findOne(String fieldName, Object value) {
        return findOne(field(fieldName).eq(value));
    }

    /**
     * Find a single document from the database, where the jOOQ Condition is
     * met.
     *
     * @param condition a custom jOOQ condition parameter
     * @return a single document or null if none was found.
     */
    public T findOne(Condition condition) {
        Record r = this.create.select().from(this.table.value()).where(condition).fetchOne();
        return recordToObject(r);
    }

    /**
     * Find multiple documents from the database, where the fields equal the
     * given value
     *
     * @param fieldName field to query for
     * @param value value you want field to be
     * @return A List<T> with all the documents that were found. If no documents
     * were found the list is empty, not null.
     */
    public List<T> find(String fieldName, Object value) {
        return find(field(fieldName).eq(value));
    }

    /**
     * Find multiple documents from the database, where the jOOQ Condition is
     * met
     *
     * @param condition a custom jOOQ condition parameter
     * @return A List<T> with all the documents that were found. If no documents
     * were found the list is empty, not null.
     */
    public List<T> find(Condition condition) {
        List<Record> rs = this.create.select().from(this.table.value()).where(condition).fetch();
        List<T> output = new ArrayList();
        for (Record r : rs) {
            T t = this.recordToObject(r);
            if (t != null) {
                output.add(t);
            }
        }
        return output;
    }

    /**
     * A Class<T> where T is the type of the Model this modelmanager is
     * managing.
     *
     * @return
     */
    public Class<T> getModelClass() {
        return this.type;
    }

    /**
     * Get the index key and value of the model
     *
     * @param instance the instance you want the key's value of
     * @return an Entry<org.jooq.Field<Object>, Object> object or null if
     * anything's wrong
     */
    public Entry<org.jooq.Field<Object>, Object> getIndex(T instance) {
        if (this.indexKey == null || instance == null) {
            return null;
        }
        if (this.indexField != null) {
            try {
                return new AbstractMap.SimpleEntry(DSL.field(this.indexKey), this.indexField.get(instance));
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                ex.printStackTrace();
            }
        } else if (this.indexMethod != null) {
            try {
                return new AbstractMap.SimpleEntry(DSL.field(this.indexKey), this.indexMethod.invoke(instance));
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Create new instance of the model, as empty/null'ed as possible
     *
     * @return new nulled instance (all values in the model will be null)
     * @throws Exception things can go wrong
     */
    public final T createEmptyInstance() throws Exception {
        final Constructor<T> constr = (Constructor<T>) this.type.getConstructors()[0];
        boolean a = constr.isAccessible();
        if (!a) {
            constr.setAccessible(true);
        }
        final List<Object> params = new ArrayList();
        for (Class<?> pType : constr.getParameterTypes()) {
            params.add((pType.isPrimitive()) ? ClassUtils.primitiveToWrapper(pType).newInstance() : null);
        }
        final T instance = constr.newInstance(params.toArray());
        constr.setAccessible(a);
        return instance;
    }

    private <T> T getCastedFieldValue(Record r, Field field, String customName) {
        if (customName == null || customName.isEmpty()) {
            customName = field.getName();
        }
        Object o = r.getValue(customName);
        if (o == null) {
            return null;
        }
        if (field.getType().isPrimitive()) {
            Object value = autoParse(o, field.getType());
            if (value != null) {
                return (T) value;
            }
        }
        try {
            return (T) field.getType().cast(o);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS = new HashMap<Class<?>, Class<?>>() {
        {
            put(boolean.class, Boolean.class);
            put(byte.class, Byte.class);
            put(char.class, Character.class);
            put(double.class, Double.class);
            put(float.class, Float.class);
            put(int.class, Integer.class);
            put(long.class, Long.class);
            put(short.class, Short.class);
            put(void.class, Void.class);
        }
    };

    private <T> T autoParse(Object o, Class<T> type) {
        Class<?> methodClass = type;
        if (PRIMITIVES_TO_WRAPPERS.containsKey(type)) {
            methodClass = PRIMITIVES_TO_WRAPPERS.get(type);
        }
        try {
            Method[] ms = methodClass.getDeclaredMethods();
            for (Method m : ms) {
                if (m.getName().startsWith("parse")
                        && m.getParameterTypes().length == 1
                        && m.getParameterTypes()[0] == String.class
                        && m.getReturnType() == type) {
                    Object res = m.invoke(null, o.toString());
                    return (T) res;
                }
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private T recordToObject(Record r) {
        if (r == null) {
            return null;
        }
        try {
            T t = this.createEmptyInstance();
            for (Entry<Field, Column> e : this.fields.entrySet()) {
                e.getKey().set(t, getCastedFieldValue(r, e.getKey(), e.getValue().value()));
            }
            return t;
        } catch (Exception ex) {
            Logger.getLogger(SQLModelManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private Map<org.jooq.Field<Object>, Object> getSerializableObject(T instance) {
        Map<org.jooq.Field<Object>, Object> c = new LinkedHashMap();
        for (Entry<Field, Column> e : fields.entrySet()) {
            try {
                if (e.getKey() == this.indexField && this.indexSetting != IndexSetting.PROVIDED) {
                    continue;
                }
                String name = e.getKey().getName();
                if (e.getValue().value() != null && !e.getValue().value().isEmpty()) {
                    name = e.getValue().value();
                }
                org.jooq.Field<Object> field = DSL.field(name);
                Object value = e.getKey().get(instance);
                c.put(field, value);
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(SQLModelManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        for (Entry<Method, CalculatedColumn> e : calculatedFields.entrySet()) {
            try {
                if (e.getKey() == this.indexMethod && this.indexSetting != IndexSetting.PROVIDED) {
                    continue;
                }
                String name = e.getValue().value();
                org.jooq.Field<Object> field = DSL.field(name);
                Object value = e.getKey().invoke(instance);
                c.put(field, value);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                Logger.getLogger(SQLModelManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return c;
    }

}
