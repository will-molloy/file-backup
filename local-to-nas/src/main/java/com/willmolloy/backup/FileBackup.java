package com.willmolloy.backup;

/**
 * File backup contract.
 *
 * @param <TSource> Source type
 * @param <TDestination> Destination type
 * @author <a href=https://willmolloy.com>Will Molloy</a>
 */
interface FileBackup<TSource, TDestination> {

  void backup(TSource source, TDestination destination);
}
