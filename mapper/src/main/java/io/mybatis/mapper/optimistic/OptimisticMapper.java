package io.mybatis.mapper.optimistic;

import io.mybatis.mapper.example.Example;
import io.mybatis.mapper.fn.Fn;
import io.mybatis.mapper.logical.LogicalMapper;
import io.mybatis.mapper.logical.LogicalProvider;
import io.mybatis.provider.Caching;
import org.apache.ibatis.annotations.*;

import java.io.Serializable;
import java.util.Optional;

/**
 * 乐观锁Mapper
 * 继承此mapper会覆盖LogicalMapper、BaseMapper、FnMapper中的删、改相关方法
 *
 * @author PING
 * @version 1.0
 * @date 2025/1/23 11:16
 */
public interface OptimisticMapper<T, I extends Serializable> extends LogicalMapper<T, I> {
  /* BaseMapper +++ */

  @Override
  @Lang(Caching.class)
  @UpdateProvider(type = OptimisticProvider.class, method = "updateByPrimaryKeySelectiveWithForceFields")
  <S extends T> int updateByPrimaryKeySelectiveWithForceFields(@Param("entity") S entity, @Param("fns") Fn.Fns<T> forceUpdateFields);


  @Override
  @Lang(Caching.class)
  @DeleteProvider(type = OptimisticProvider.class, method = "deleteByPrimaryKey")
  int deleteByPrimaryKey(I id);

  @Override
  @Lang(Caching.class)
  @DeleteProvider(type = OptimisticProvider.class, method = "delete")
  int delete(T entity);

  @Override
  @Lang(Caching.class)
  @UpdateProvider(type = OptimisticProvider.class, method = "updateByPrimaryKey")
  <S extends T> int updateByPrimaryKey(S entity);

  @Override
  @Lang(Caching.class)
  @UpdateProvider(type = OptimisticProvider.class, method = "updateByPrimaryKeySelective")
  <S extends T> int updateByPrimaryKeySelective(S entity);

  @Override
  @Lang(Caching.class)
  @UpdateProvider(type = OptimisticProvider.class, method = "updateByExample")
  <S extends T> int updateByExample(@Param("entity") S entity, @Param("example") Example<T> example);

  @Override
  @Lang(Caching.class)
  @UpdateProvider(type = OptimisticProvider.class, method = "updateByExampleSelective")
  <S extends T> int updateByExampleSelective(@Param("entity") S entity, @Param("example") Example<T> example);

}
