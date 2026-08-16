import { Component, OnInit, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router, RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-cart',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './cart.html'
})
export class CartComponent implements OnInit {
  cartItems = signal<any[]>([]);
  loading = signal<boolean>(true);
  checkingOut = signal<boolean>(false);
  successMessage = signal<boolean>(false);

  // Computed properties
  totalItems = computed(() => {
    return this.cartItems().reduce((acc, item) => acc + item.quantity, 0);
  });

  grandTotal = computed(() => {
    const total = this.cartItems().reduce((acc, item) => {
      const price = item.product?.price || 0;
      return acc + (price * item.quantity);
    }, 0);
    return Number(total.toFixed(2));
  });

  constructor(
    public readonly apiService: ApiService,
    private readonly router: Router
  ) {}

  ngOnInit() {
    if (!this.apiService.currentUser()) {
      this.router.navigate(['/login']);
      return;
    }
    this.loadCart();
  }

  loadCart() {
    this.loading.set(true);
    this.apiService.getCart().subscribe({
      next: (items) => {
        this.cartItems.set(items);
        this.loading.set(false);
      },
      error: (err) => {
        console.error('Failed to load cart', err);
        this.loading.set(false);
      }
    });
  }

  incrementQuantity(item: any) {
    this.apiService.addToCart(item.product_id, 1, false).subscribe({
      next: () => {
        this.loadCart();
      },
      error: (err) => console.error('Error incrementing item qty', err)
    });
  }

  decrementQuantity(item: any) {
    this.apiService.removeFromCart(item.product_id, 1).subscribe({
      next: () => {
        this.loadCart();
      },
      error: (err) => console.error('Error decrementing item qty', err)
    });
  }

  removeItem(item: any) {
    this.apiService.removeFromCart(item.product_id).subscribe({
      next: () => {
        this.loadCart();
      },
      error: (err) => console.error('Error removing item from cart', err)
    });
  }

  onCheckout() {
    if (this.cartItems().length === 0) return;
    this.checkingOut.set(true);
    
    this.apiService.checkout().subscribe({
      next: () => {
        this.checkingOut.set(false);
        this.cartItems.set([]);
        this.successMessage.set(true);
      },
      error: (err) => {
        this.checkingOut.set(false);
        console.error('Checkout failed', err);
        alert('Transaction failed. Please try again.');
      }
    });
  }
}
