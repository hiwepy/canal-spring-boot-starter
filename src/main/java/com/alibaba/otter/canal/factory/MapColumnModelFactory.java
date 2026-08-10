package com.alibaba.otter.canal.factory;

import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.BeanUtils;

import java.util.Map;

/**
 * {@link IModelFactory} that materialises flat Canal column maps
 * ({@code Map<String, String>}) into entry model instances.
 * <p>
 * The target entity class is resolved from the handler's generic signature and
 * populated using its MyBatis-Plus {@link TableInfo} column-to-property mapping.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class MapColumnModelFactory extends AbstractModelFactory<Map<String, String>> {

    @Override
    <R> R newInstance(Class<R> tableClass, Map<String, String> valueMap) throws Exception {
        R object = BeanUtils.instantiateClass(tableClass);
        // Resolve the MyBatis-Plus table metadata.
        TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
        // Iterate over mapped fields.
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            // Map the column value onto the matching property.
            Object value = MapUtils.getObject(valueMap, tableFieldInfo.getColumn());
            PropertyUtils.setProperty(object, tableFieldInfo.getProperty(), value);
        }
        return object;
    }

}
