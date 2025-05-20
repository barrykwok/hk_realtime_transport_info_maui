using System.Collections.ObjectModel;
using System.Linq;
using hk_realtime_transport_info_maui.Models;
using hk_realtime_transport_info_maui.Services;
using Microsoft.Maui.Controls;
using Microsoft.Extensions.Logging;

namespace hk_realtime_transport_info_maui
{
    public partial class FavoriteRoutesPage : ContentPage
    {
        private readonly LiteDbService _databaseService;
        private readonly EtaService _etaService;
        private readonly ILoggerFactory _loggerFactory;
        public ObservableCollection<TransportRoute> FavoriteRoutes { get; set; } = new();

        public FavoriteRoutesPage(LiteDbService databaseService, EtaService etaService, ILoggerFactory loggerFactory)
        {
            InitializeComponent();
            _databaseService = databaseService;
            _etaService = etaService;
            _loggerFactory = loggerFactory;
            BindingContext = this;
            LoadFavorites();
        }

        private void LoadFavorites()
        {
            var favs = _databaseService.GetAllRecords<FavoriteRoute>("FavoriteRoutes").ToList();
            var allRoutes = _databaseService.GetAllRecords<TransportRoute>("TransportRoutes").ToList();
            var favRoutes = favs.Select(f => allRoutes.FirstOrDefault(r => r.Id == f.RouteId)).Where(r => r != null).ToList();
            FavoriteRoutes.Clear();
            foreach (var route in favRoutes)
                FavoriteRoutes.Add(route);
        }

        private async void OnFavoriteSelected(object sender, SelectionChangedEventArgs e)
        {
            if (e.CurrentSelection.FirstOrDefault() is TransportRoute selectedRoute)
            {
                // Navigate to RouteDetailsPage for the selected route
                var logger = _loggerFactory.CreateLogger<RouteDetailsPage>();
                var detailsPage = new RouteDetailsPage(_databaseService, _etaService, logger);
                detailsPage.SetRoute(selectedRoute);
                await Navigation.PushAsync(detailsPage);
                FavoritesCollection.SelectedItem = null;
            }
        }

        public void ReloadFavorites()
        {
            LoadFavorites();
        }
    }
} 