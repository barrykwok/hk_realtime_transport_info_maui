using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Represents a dynamic data collection that provides notifications when items get added, removed, or when the whole list is refreshed.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ObservableRangeCollection<T> : ObservableCollection<T>
    {
        private bool _suppressNotification = false;
        private bool _isInBatchMode = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObservableRangeCollection{T}"/> class.
        /// </summary>
        public ObservableRangeCollection() : base() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ObservableRangeCollection{T}"/> class that contains elements copied from the specified collection.
        /// </summary>
        /// <param name="collection">The collection from which the elements are copied.</param>
        public ObservableRangeCollection(IEnumerable<T> collection) : base(collection) { }

        /// <summary>
        /// Begins a batch operation where collection change notifications are suppressed until EndBatch is called.
        /// </summary>
        public void BeginBatch()
        {
            _isInBatchMode = true;
            _suppressNotification = true;
        }

        /// <summary>
        /// Ends a batch operation and raises a Reset notification.
        /// </summary>
        public void EndBatch()
        {
            _isInBatchMode = false;
            _suppressNotification = false;
            
            // Raise a reset notification to update the UI once
            OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
            OnPropertyChanged(new PropertyChangedEventArgs("Count"));
            OnPropertyChanged(new PropertyChangedEventArgs("Item[]"));
        }

        /// <summary>
        /// Adds the elements of the specified collection to the end of the <see cref="ObservableRangeCollection{T}"/>.
        /// </summary>
        /// <param name="collection">The collection whose elements should be added to the end of the <see cref="ObservableRangeCollection{T}"/>.</param>
        /// <param name="notificationMode">The notification mode for the changes.</param>
        public void AddRange(IEnumerable<T> collection, NotifyCollectionChangedAction notificationMode = NotifyCollectionChangedAction.Add)
        {
            if (collection == null)
                throw new System.ArgumentNullException(nameof(collection));

            CheckReentrancy();

            if (_suppressNotification)
            {
                // If in batch mode, just add the items without notifications
                foreach (var i in collection)
                {
                    Items.Add(i);
                }
                return;
            }

            if (notificationMode == NotifyCollectionChangedAction.Reset)
            {
                foreach (var i in collection)
                {
                    Items.Add(i);
                }

                OnPropertyChanged(new PropertyChangedEventArgs("Count"));
                OnPropertyChanged(new PropertyChangedEventArgs("Item[]"));
                OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));

                return;
            }

            int startIndex = Count;
            var changedItems = collection is List<T> ? (List<T>)collection : new List<T>(collection);
            foreach (var i in changedItems)
            {
                Items.Add(i);
            }

            OnPropertyChanged(new PropertyChangedEventArgs("Count"));
            OnPropertyChanged(new PropertyChangedEventArgs("Item[]"));
            OnCollectionChanged(new NotifyCollectionChangedEventArgs(notificationMode, changedItems, startIndex));
        }

        /// <summary>
        /// Removes the specified items from the <see cref="ObservableRangeCollection{T}"/>.
        /// </summary>
        /// <param name="collection">The collection of items to be removed.</param>
        public void RemoveRange(IEnumerable<T> collection)
        {
            if (collection == null)
                throw new System.ArgumentNullException(nameof(collection));

            CheckReentrancy();

            if (_suppressNotification)
            {
                // If in batch mode, just remove the items without notifications
                foreach (var i in collection)
                {
                    Items.Remove(i);
                }
                return;
            }

            var changedItems = collection is List<T> ? (List<T>)collection : new List<T>(collection);
            foreach (var i in changedItems)
            {
                Items.Remove(i);
            }

            OnPropertyChanged(new PropertyChangedEventArgs("Count"));
            OnPropertyChanged(new PropertyChangedEventArgs("Item[]"));
            OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
        }

        /// <summary>
        /// Clears the current collection and replaces it with the specified collection.
        /// </summary>
        /// <param name="collection">The items to replace the collection with.</param>
        public void ReplaceRange(IEnumerable<T> collection)
        {
            if (collection == null)
                throw new System.ArgumentNullException(nameof(collection));

            CheckReentrancy();

            if (_suppressNotification)
            {
                // If in batch mode, just replace the items without notifications
                Items.Clear();
                foreach (var i in collection)
                {
                    Items.Add(i);
                }
                return;
            }

            Items.Clear();
            var changedItems = collection is List<T> ? (List<T>)collection : new List<T>(collection);
            foreach (var i in changedItems)
            {
                Items.Add(i);
            }

            OnPropertyChanged(new PropertyChangedEventArgs("Count"));
            OnPropertyChanged(new PropertyChangedEventArgs("Item[]"));
            OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
        }
        
        /// <summary>
        /// Override the base Add method to suppress notifications during batch operations
        /// </summary>
        protected override void OnCollectionChanged(NotifyCollectionChangedEventArgs e)
        {
            if (!_suppressNotification)
                base.OnCollectionChanged(e);
        }
        
        /// <summary>
        /// Override the base OnPropertyChanged method to suppress notifications during batch operations
        /// </summary>
        protected override void OnPropertyChanged(PropertyChangedEventArgs e)
        {
            if (!_suppressNotification)
                base.OnPropertyChanged(e);
        }
    }
} 